# B+ツリーKVS 実装設計書 (libc非依存・データ指向設計)

本ドキュメントは、現行の `src/nostr/db/` をlibc非依存な高速B+ツリーKVSに再実装するための設計仕様とTODOリストである。

---

## 現行実装の課題分析

| 課題 | 現行実装 | 影響 |
|------|---------|------|
| 線形スキャン | `db_event.c` の `get_event_by_id` / `delete_event` がO(N)線形スキャン | イベント増加で致命的な性能劣化 |
| ハッシュテーブル固定サイズ | IDインデックスがmmap固定サイズ (16MB)、リハッシュ未実装 | 容量上限に達すると挿入不能 |
| タイムライン挿入O(N) | ソート済み配列への挿入で全要素シフト | 書き込みスループット劣化 |
| クラッシュ耐性なし | WAL/ジャーナル未実装、msyncに依存 | 電源断でデータ不整合 |
| バッファ管理なし | mmap丸ごとマッピングでOSページキャッシュに依存 | メモリ使用量の制御不能、リカバリとの整合性欠如 |
| ファイル拡張未実装 | `NOSTR_DB_ERROR_FULL` を返すだけ | 64MBを超えるデータ保存不可 |
| 重複キーの扱い | pubkey/kind/tagのリンクリストがmmapプール内に線形伸長 | フラグメンテーション、範囲検索が非効率 |

---

## 新アーキテクチャ概要

```text
┌─────────────────────────────────────────────────────────┐
│                    Public API (db.h)                     │
│  nostr_db_init / write_event / query / delete / shutdown │
├─────────────────────────────────────────────────────────┤
│              Query Engine (query/)                       │
│  フィルタ解析 → 戦略選択 → B+ツリー検索 → 結果構築      │
├─────────────────────────────────────────────────────────┤
│             B+ツリー インデックスレイヤー                 │
│  ID(ハッシュ) / Timeline / Pubkey / Kind / Tag (B+ツリー) │
├─────────────────────────────────────────────────────────┤
│             レコード管理レイヤー (record/)               │
│  スロットページ / 可変長レコード / RID                    │
├─────────────────────────────────────────────────────────┤
│           WAL & リカバリレイヤー (wal/)                  │
│  Write-Ahead Log / チェックポイント / Undo-Redo          │
├─────────────────────────────────────────────────────────┤
│           バッファプールレイヤー (buffer/)               │
│  ページキャッシュ / ピン留め / Clock置換                  │
├─────────────────────────────────────────────────────────┤
│           ディスク管理レイヤー (disk/)                   │
│  ページI/O / 空きページ管理 / ファイル拡張               │
└─────────────────────────────────────────────────────────┘
         ↓ syscall (O_DIRECT / pread / pwrite / mmap)
    ┌──────────────────────┐
    │  src/arch/linux/x86_64 │
    └──────────────────────┘
```

### データ指向設計 (DOD) の原則

1. **Structure of Arrays (SoA)**: ホットデータとコールドデータを分離。B+ツリーノード内はキー配列と子ポインタ配列を連続配置
1. **キャッシュライン最適化**: ページサイズ4096B、ノードサイズ=ページサイズ。リーフノード内のキーは連続メモリ領域に配置してプリフェッチ効率を最大化
1. **ゼロコピー参照**: バッファプール上のページをピン留めして直接参照。不要なmemcpyを排除
1. **バッチ処理指向**: 複数インデックス更新をWALに一括記録し、ディスクI/Oを最小化

---

## Phase 1: ディスク管理レイヤー (`src/nostr/db/disk/`)

ストレージをページ (4096バイト固定長ブロック) 単位で管理する最下層。

### 1.1 ページI/O

#### 構造体

```c
// ページ番号型 (0 = 無効)
typedef uint32_t page_id_t;
#define PAGE_ID_NULL ((page_id_t)0)

// ページサイズ
#define DB_PAGE_SIZE 4096

// ページデータ (4096バイト、アライメント保証)
typedef struct {
  uint8_t data[DB_PAGE_SIZE] __attribute__((aligned(DB_PAGE_SIZE)));
} PageData;

// ファイルヘッダ (1ページ目に配置)
typedef struct {
  char       magic[8];           // ファイル識別子
  uint32_t   version;            // スキーマバージョン
  uint32_t   page_size;          // DB_PAGE_SIZE (4096)
  uint32_t   total_pages;        // ファイル内総ページ数
  page_id_t  free_list_head;     // フリーリスト先頭ページ
  uint32_t   free_page_count;    // 空きページ数
  uint64_t   next_lsn;           // 次に発番するLSN
  page_id_t  wal_checkpoint_lsn; // 最終チェックポイントLSN
  uint8_t    reserved[4024];     // 4096 - ヘッダフィールド分
} FileHeader;
_Static_assert(sizeof(FileHeader) == DB_PAGE_SIZE, "FileHeader must be one page");

// ディスクマネージャ
typedef struct {
  int32_t    fd;                 // ファイルディスクリプタ
  FileHeader header;             // キャッシュ済みヘッダ
  char       path[256];          // ファイルパス
} DiskManager;
```

#### API

```c
// 初期化・終了
NostrDBError disk_manager_open(DiskManager* dm, const char* path);
NostrDBError disk_manager_create(DiskManager* dm, const char* path, uint32_t initial_pages);
void         disk_manager_close(DiskManager* dm);

// ページ読み書き (O_DIRECT対応、ページアライメント済みバッファ必須)
NostrDBError disk_read_page(DiskManager* dm, page_id_t page_id, PageData* out);
NostrDBError disk_write_page(DiskManager* dm, page_id_t page_id, const PageData* data);
NostrDBError disk_sync(DiskManager* dm);

// ページ割り当て・解放
page_id_t    disk_alloc_page(DiskManager* dm);
NostrDBError disk_free_page(DiskManager* dm, page_id_t page_id);

// ファイル拡張 (ftruncate → total_pages 更新)
NostrDBError disk_extend(DiskManager* dm, uint32_t additional_pages);
```

### 1.2 空きページ管理 (フリーリスト)

フリーリスト方式を採用。空きページの先頭4バイトに次の空きページIDを書き込むチェーン構造。

```c
// 空きページの先頭レイアウト
typedef struct {
  page_id_t next_free;            // 次の空きページID (0 = 末尾)
  uint8_t   padding[DB_PAGE_SIZE - sizeof(page_id_t)];
} FreePageEntry;
```

**割り当てアルゴリズム**:

1. `header.free_list_head != 0` → フリーリストの先頭ページを返し、先頭を次に繋ぎ替え
1. フリーリスト空 → `disk_extend()` でファイルを拡張し新規ページを返す

**解放アルゴリズム**:

1. 解放ページの先頭に現在の `free_list_head` を書き込み
1. `free_list_head` を解放ページIDに更新

### TODO Phase 1

- [x] **1-1**: `disk/disk_types.h` — `page_id_t`, `PageData`, `FileHeader`, `FreePageEntry` 型定義
- [x] **1-2**: `disk/disk_manager.h` — `DiskManager` 構造体、API宣言
- [x] **1-3**: `disk/disk_manager.c` — `disk_manager_open`, `disk_manager_create`, `disk_manager_close` 実装
    - `internal_open()` で `O_RDWR | O_DSYNC` フラグ使用
    - 新規作成時は `internal_ftruncate()` で初期サイズ確保、ヘッダページ書き込み
- [x] **1-4**: `disk/disk_io.c` — `disk_read_page`, `disk_write_page`, `disk_sync` 実装
    - `internal_pread()` / `internal_pwrite()` でオフセット = `page_id * DB_PAGE_SIZE`
    - アライメントチェック (`PageData` は `__attribute__((aligned))`)
- [x] **1-5**: `disk/disk_alloc.c` — `disk_alloc_page`, `disk_free_page`, `disk_extend` 実装
    - フリーリストチェーン操作
    - 拡張時は128ページ単位 (512KB) でバッチ拡張
- [x] **1-6**: `tests/disk-test.cpp` — ページ読み書き、割り当て・解放・拡張テスト

---

## Phase 2: バッファプールレイヤー (`src/nostr/db/buffer/`)

ディスクページのインメモリキャッシュ。全ての上位レイヤーはバッファプール経由でページにアクセスする。

### 2.1 バッファプール構造

```c
#define BUFFER_POOL_DEFAULT_SIZE 4096  // 4096ページ = 16MB

// バッファフレームの状態 (SoA: ホットデータ分離)
typedef struct {
  // --- ホットデータ (頻繁にアクセス) ---
  page_id_t* page_ids;      // [pool_size] 各フレームに割り当てられたページID
  uint8_t*   pin_counts;    // [pool_size] ピン留めカウント
  uint8_t*   dirty_flags;   // [pool_size] ダーティフラグ
  uint8_t*   ref_bits;      // [pool_size] Clock参照ビット

  // --- データ領域 ---
  PageData*  pages;          // [pool_size] 実ページデータ (アライメント済み)

  // --- コールドデータ ---
  uint64_t*  lsn;            // [pool_size] 最終変更LSN

  // --- メタデータ ---
  uint32_t   pool_size;      // 総フレーム数
  uint32_t   clock_hand;     // Clock置換アルゴリズムの現在位置

  // --- ページID → フレームIndex のハッシュマップ ---
  uint32_t*  hash_table;     // [hash_size] オープンアドレスハッシュテーブル
  uint32_t   hash_size;      // ハッシュテーブルサイズ (pool_sizeの2倍程度)

  // --- ディスクマネージャ参照 ---
  DiskManager* disk;
} BufferPool;
```

**データ指向ポイント**:

- `page_ids`, `pin_counts`, `dirty_flags`, `ref_bits` は別々の配列 (SoA)。Clock置換時にはこれらの小さい配列のみをシーケンシャルスキャンするため、キャッシュライン利用効率が最大化される
- `pages` (各4KB) とメタデータが混在しないためプリフェッチが効く

### 2.2 Clock置換アルゴリズム

LRUに近い性能を低オーバーヘッドで実現。

```text
clock_hand → [frame0] → [frame1] → ... → [frameN-1] → [frame0] ...

置換フロー:

1. clock_handの位置から巡回開始
1. pin_count > 0 → スキップ (使用中)
1. ref_bit == 1 → ref_bit = 0 にして次へ (セカンドチャンス)
1. ref_bit == 0 → このフレームを犠牲者として選択
1. dirty_flag == 1 → ディスクに書き戻してからフレームを再利用
```

### 2.3 API

```c
// 初期化・終了
NostrDBError buffer_pool_init(BufferPool* pool, DiskManager* disk, uint32_t pool_size);
void         buffer_pool_shutdown(BufferPool* pool);

// ページアクセス (ピン留め)
// pin: ページをバッファプールに読み込み、ピン留めしてポインタを返す
// 未キャッシュの場合はディスクから読み込み
PageData*    buffer_pool_pin(BufferPool* pool, page_id_t page_id);

// 新規ページ割り当て (ディスク割り当て + バッファに配置)
page_id_t    buffer_pool_alloc_page(BufferPool* pool, PageData** out);

// アンピン (使用完了通知)
void         buffer_pool_unpin(BufferPool* pool, page_id_t page_id);

// ダーティマーク
void         buffer_pool_mark_dirty(BufferPool* pool, page_id_t page_id, uint64_t lsn);

// フラッシュ
NostrDBError buffer_pool_flush(BufferPool* pool, page_id_t page_id);
NostrDBError buffer_pool_flush_all(BufferPool* pool);
```

### TODO Phase 2

- [x] **2-1**: `buffer/buffer_types.h` — バッファプール型定義 (SoA構造)
- [x] **2-2**: `buffer/buffer_pool.h` — API宣言
- [x] **2-3**: `buffer/buffer_pool.c` — `buffer_pool_init` / `buffer_pool_shutdown`
    - `internal_mmap(MAP_PRIVATE | MAP_ANONYMOUS)` で SoA 各配列を一括確保
    - ハッシュテーブルサイズ = pool_size * 2 (素数に丸め)
- [x] **2-4**: `buffer/buffer_pool.c` — `buffer_pool_pin` 実装
    - ハッシュテーブルでフレーム検索 → ヒットならpin_count++、ref_bit=1
    - ミスならClock置換で犠牲フレーム選択 → ディスクから読み込み
- [x] **2-5**: `buffer/buffer_pool.c` — `buffer_pool_alloc_page` 実装
    - `disk_alloc_page()` → 新規ページIDを取得 → フレームに配置
- [x] **2-6**: `buffer/buffer_pool.c` — `buffer_pool_unpin`, `buffer_pool_mark_dirty`, `buffer_pool_flush` 実装
    - WALプロトコル: フラッシュ前に `lsn <= wal_flushed_lsn` を確認
- [x] **2-7**: `tests/buffer-test.cpp` — ピン・アンピン・置換・フラッシュのテスト

---

## Phase 3: WAL & リカバリレイヤー (`src/nostr/db/wal/`)

Write-Ahead Loggingによるクラッシュ耐性の保証。

### 3.1 WALレコード構造

```c
// Log Sequence Number
typedef uint64_t lsn_t;
#define LSN_NULL ((lsn_t)0)

// WALレコード種別
typedef enum {
  WAL_RECORD_BEGIN      = 1,  // トランザクション開始
  WAL_RECORD_COMMIT     = 2,  // コミット
  WAL_RECORD_ABORT      = 3,  // アボート
  WAL_RECORD_UPDATE     = 4,  // ページ更新 (before/after image)
  WAL_RECORD_ALLOC_PAGE = 5,  // ページ割り当て
  WAL_RECORD_FREE_PAGE  = 6,  // ページ解放
  WAL_RECORD_CHECKPOINT = 7,  // チェックポイント
} WalRecordType;

// WALレコードヘッダ (固定長32バイト)
typedef struct {
  lsn_t         lsn;            // このレコードのLSN
  lsn_t         prev_lsn;       // 同一トランザクションの前レコードLSN
  uint32_t      tx_id;          // トランザクションID
  WalRecordType type;           // レコード種別
  uint16_t      data_length;    // 可変長データサイズ
  uint16_t      padding;
} WalRecordHeader;
_Static_assert(sizeof(WalRecordHeader) == 32, "WalRecordHeader must be 32 bytes");

// UPDATE レコード用ペイロード
typedef struct {
  page_id_t page_id;            // 対象ページ
  uint16_t  offset;             // ページ内オフセット
  uint16_t  length;             // 変更サイズ
  // uint8_t old_data[length];  // 変更前データ (Undo用)
  // uint8_t new_data[length];  // 変更後データ (Redo用)
} WalUpdatePayload;

// WALマネージャ
typedef struct {
  int32_t   fd;                 // WALファイルディスクリプタ
  lsn_t     flushed_lsn;       // ディスクにフラッシュ済みの最大LSN
  lsn_t     next_lsn;          // 次に発番するLSN
  uint32_t  next_tx_id;        // 次のトランザクションID

  // WALバッファ (フラッシュ前のレコードを蓄積)
  uint8_t*  buffer;             // WALバッファ (64KBなど)
  uint32_t  buffer_size;        // バッファ容量
  uint32_t  buffer_used;        // 使用量

  // アクティブトランザクション追跡
  uint32_t  active_tx[64];      // アクティブTX IDリスト
  uint8_t   active_tx_count;    // アクティブTX数
} WalManager;
```

### 3.2 WALプロトコル

1. **WAL原則**: バッファプールがダーティページをフラッシュする前に、そのページのLSN以下の全WALレコードがディスクに書き込まれていることを保証
1. **コミット順序**: COMMITレコードがディスクにフラッシュされて初めてトランザクション完了
1. **グループコミット**: 複数トランザクションのCOMMITを1回のfsyncでまとめて永続化

### 3.3 リカバリアルゴリズム

```text
起動時リカバリ:

1. WALファイルを先頭から読み込み
1. 最終チェックポイントレコードを特定
1. Analysis Phase: チェックポイント以降のレコードから
    -  アクティブトランザクションリスト構築
    -  ダーティページリスト構築
1. Redo Phase: チェックポイント以降の全UPDATEレコードを再適用
    -  ページLSN < レコードLSN のページのみ再適用
1. Undo Phase: コミットされていないトランザクションのUPDATEを逆順にロールバック
1. WALファイルをトランケート (チェックポイント以前を削除)
```

### 3.4 シングルスレッド簡略化

libelayはシングルスレッド (epollイベントループ) であるため:

- ロックマネージャは不要
- MVCC は不要
- トランザクション並行制御は不要
- WALの目的はクラッシュリカバリのみに特化

### TODO Phase 3

- [x] **3-1**: `wal/wal_types.h` — LSN型、レコード種別、レコードヘッダ、ペイロード型定義
- [x] **3-2**: `wal/wal_manager.h` — `WalManager` 構造体、API宣言
- [x] **3-3**: `wal/wal_manager.c` — `wal_init`, `wal_shutdown` 実装
    - WALファイル (`data/wal.log`) のオープン・作成
    - WALバッファ確保 (`internal_mmap` で64KB匿名マップ)
- [x] **3-4**: `wal/wal_write.c` — WALレコード書き込み
    - `wal_log_begin(tx_id)` → BEGINレコード追記
    - `wal_log_update(tx_id, page_id, offset, length, old_data, new_data)` → UPDATEレコード追記
    - `wal_log_commit(tx_id)` → COMMITレコード追記 + バッファフラッシュ
    - `wal_log_abort(tx_id)` → ABORTレコード追記
- [x] **3-5**: `wal/wal_flush.c` — WALバッファのディスクフラッシュ
    - `wal_flush()` → バッファ内容をpwrite + fsync、`flushed_lsn` 更新
- [x] **3-6**: `wal/wal_recovery.c` — 起動時リカバリ実装
    - `wal_recover(WalManager*, DiskManager*)` → Redo/Undoフェーズ実行
- [x] **3-7**: `wal/wal_checkpoint.c` — チェックポイント
    - `wal_checkpoint(WalManager*, BufferPool*)` → ダーティページフラッシュ + CHECKPOINTレコード
    - 一定イベント数ごとに非静止チェックポイントを実行
- [x] **3-8**: `tests/wal-test.cpp` — WALレコード書き込み・読み込み・リカバリテスト

---

## Phase 4: レコード管理レイヤー (`src/nostr/db/record/`)

可変長のNostrイベントデータを固定長ページ内に効率的に格納する。

### 4.1 スロットページ構造

```c
// Record Identifier (ページ番号 + スロット番号)
typedef struct {
  page_id_t page_id;       // ページID
  uint16_t  slot_index;    // ページ内スロット番号
} RecordId;

// ページ種別
typedef enum {
  PAGE_TYPE_FREE        = 0,
  PAGE_TYPE_FILE_HEADER = 1,
  PAGE_TYPE_RECORD      = 2,   // レコード格納ページ
  PAGE_TYPE_BTREE_LEAF  = 3,   // B+ツリーリーフ
  PAGE_TYPE_BTREE_INNER = 4,   // B+ツリー内部ノード
  PAGE_TYPE_OVERFLOW    = 5,   // オーバーフローページ
} PageType;

// スロットページヘッダ (ページ先頭に配置)
typedef struct {
  page_id_t page_id;           // 自身のページID
  PageType  page_type;         // ページ種別 (uint8_t)
  uint8_t   flags;             // フラグ
  uint16_t  slot_count;        // 使用中スロット数
  uint16_t  free_space_start;  // 空き領域開始オフセット (IDテーブル末尾)
  uint16_t  free_space_end;    // 空き領域終了オフセット (レコード先頭)
  uint16_t  fragmented_space;  // フラグメンテーションで失われた空間
  page_id_t overflow_page;     // オーバーフローページID (スパンドレコード用)
  uint8_t   reserved[6];
} SlotPageHeader;
_Static_assert(sizeof(SlotPageHeader) == 24, "SlotPageHeader must be 24 bytes");

// スロットディレクトリエントリ (4バイト)
typedef struct {
  uint16_t offset;    // ページ内のレコード開始オフセット (0 = 削除済み)
  uint16_t length;    // レコード長
} SlotEntry;

// ページレイアウト:
// [SlotPageHeader (24B)] [SlotEntry[0]] [SlotEntry[1]] ... → ← [Record data] ... [Record data]
//                        ↑ free_space_start                    ↑ free_space_end
```

### 4.2 レコードフォーマット (イベントデータ)

```c
// イベントレコードヘッダ (固定長 152バイト)
typedef struct {
  uint8_t  id[32];           // Event ID (raw bytes)
  uint8_t  pubkey[32];       // Public key (raw bytes)
  uint8_t  sig[64];          // Signature (raw bytes)
  int64_t  created_at;       // Unix timestamp
  uint32_t kind;             // Event type
  uint32_t flags;            // bit0 = deleted
  uint16_t content_length;   // Content length
  uint16_t tags_length;      // Serialized tags length
  // Followed by:
  // uint8_t content[content_length];
  // uint8_t tags[tags_length];
} EventRecord;
```

### 4.3 スパンドレコード (ページ超えデータ)

大きなイベント (content + tags > ~4000バイト) は複数ページにまたがる。

```text
[Primary Page]
  SlotEntry → { offset, length=SPANNED_MARKER }
  レコード先頭部分 + overflow_page_id

[Overflow Page 1]
  page_type = PAGE_TYPE_OVERFLOW
  データ続き + next_overflow_page_id

[Overflow Page N]
  データ残り、next_overflow_page_id = 0
```

### 4.4 コンパクション

削除によるフラグメンテーションが閾値を超えた場合、ページ内レコードを末尾方向に詰め直す。SlotEntryのoffsetを更新するだけで外部参照 (RID) は不変。

### TODO Phase 4

- [x] **4-1**: `record/record_types.h` — `RecordId`, `PageType`, `SlotPageHeader`, `SlotEntry`, `EventRecord` 型定義
- [x] **4-2**: `record/slot_page.h` — スロットページ操作API宣言
- [x] **4-3**: `record/slot_page.c` — スロットページ操作実装
    - `slot_page_init(PageData*, page_id_t)` — ページ初期化
    - `slot_page_insert(PageData*, const void* data, uint16_t length, uint16_t* slot_index)` — レコード挿入
    - `slot_page_read(const PageData*, uint16_t slot_index, void* out, uint16_t* length)` — レコード読み出し
    - `slot_page_delete(PageData*, uint16_t slot_index)` — レコード削除 (スロットをマーク)
    - `slot_page_free_space(const PageData*)` — 利用可能空き容量
    - `slot_page_compact(PageData*)` — フラグメンテーション解消
- [x] **4-4**: `record/record_manager.h` / `record/record_manager.c` — レコードマネージャ
    - `record_insert(BufferPool*, const void* data, uint16_t length, RecordId* out_rid)` — レコード挿入 (空きページ検索含む)
    - `record_read(BufferPool*, RecordId rid, void* out, uint16_t* length)` — レコード読み出し
    - `record_delete(BufferPool*, RecordId rid)` — レコード削除
    - `record_update(BufferPool*, RecordId rid, const void* data, uint16_t length)` — レコード更新 (サイズ変更はdelete+insert)
- [x] **4-5**: `record/overflow.c` — スパンドレコード (オーバーフローページチェーン) 実装
- [x] **4-6**: `record/event_serializer.c` — `NostrEventEntity` ↔ `EventRecord` 変換
    - hex文字列 → バイナリ変換含む
    - タグのバイナリシリアライゼーション (既存の `db_tags.c` を流用)
- [x] **4-7**: `tests/record-test.cpp` — スロットページ挿入・読み出し・削除・コンパクション・オーバーフローテスト

---

## Phase 5: B+ツリー インデックスレイヤー (`src/nostr/db/btree/`)

全インデックスの基盤となる汎用B+ツリー実装。

### 5.1 B+ツリーノード構造

```c
// B+ツリーメタデータ (ルートページに配置)
typedef struct {
  page_id_t root_page;       // ルートノードのページID
  uint32_t  height;          // ツリーの高さ (1 = リーフのみ)
  uint32_t  entry_count;     // 総エントリ数
  uint32_t  leaf_count;      // リーフノード数
  uint32_t  inner_count;     // 内部ノード数
  uint16_t  key_size;        // キーサイズ (バイト)
  uint16_t  value_size;      // 値サイズ (バイト)
  uint8_t   key_type;        // キー型 (比較関数選択用)
  uint8_t   flags;
  uint8_t   reserved[10];
} BTreeMeta;

// B+ツリー内部ノードページレイアウト
// [PageHeader(24B)] [BTreeNodeHeader] [child_0] [key_0] [child_1] [key_1] ... [child_N]
//
// child_i は key_{i-1} <= x < key_i の範囲を担当するサブツリーのページID
typedef struct {
  uint16_t  key_count;        // キー数 (子ポインタ数 = key_count + 1)
  uint8_t   is_leaf;          // 0 = 内部ノード
  uint8_t   reserved;
  page_id_t right_sibling;    // 右隣ノード (内部ノードでは未使用、0)
} BTreeNodeHeader;

// B+ツリーリーフノードページレイアウト
// [PageHeader(24B)] [BTreeNodeHeader] [key_0, value_0] [key_1, value_1] ... [key_N, value_N]
//
// リーフ間は right_sibling でリンクリスト形成 (範囲スキャン用)
```

**データ指向ポイント**:

- 内部ノード: キー配列と子ポインタ配列を分離配置。検索時はキー配列のみシーケンシャルアクセスでバイナリサーチ → キャッシュヒット率向上
- リーフノード: `[keys...][values...]` のSoA配置。キーだけの検索時にvalueをキャッシュラインに載せない

```text
内部ノードの物理レイアウト (SoA):
[SlotPageHeader][BTreeNodeHeader][key_0][key_1]...[key_N-1]  |  [child_0][child_1]...[child_N]
                                 ←  キー配列 (連続)  →         ←  子ポインタ配列 (連続)  →

リーフノードの物理レイアウト (SoA):
[SlotPageHeader][BTreeNodeHeader][key_0][key_1]...[key_N-1]  |  [val_0][val_1]...[val_N-1]  | right_sibling
                                 ←  キー配列 (連続)  →         ←  値配列 (連続)  →
```

### 5.2 キー型定義

```c
// キー比較関数型
typedef int32_t (*BTreeKeyCompare)(const void* a, const void* b, uint16_t key_size);

// キー型
typedef enum {
  BTREE_KEY_BYTES32  = 0,  // 32バイト固定 (ID, pubkey)
  BTREE_KEY_INT64    = 1,  // int64_t (created_at)
  BTREE_KEY_UINT32   = 2,  // uint32_t (kind)
  BTREE_KEY_COMPOSITE = 3, // 複合キー (pubkey+kind, tag_name+tag_value)
} BTreeKeyType;

// 各インデックスのキー・値定義
// ID Index:       key=id[32],         value=RecordId
// Timeline Index: key=created_at(8B), value=RecordId
// Pubkey Index:   key=pubkey[32],     value=RecordId
// Kind Index:     key=kind(4B),       value=RecordId
// PK+Kind Index:  key=pubkey[32]+kind(4B)=36B, value=RecordId
// Tag Index:      key=tag_name(1B)+tag_value[32]=33B, value=RecordId
```

### 5.3 ノード容量計算

```text
ページ利用可能領域 = 4096 - sizeof(SlotPageHeader) - sizeof(BTreeNodeHeader)
                   = 4096 - 24 - 8 = 4064 bytes

内部ノード (ID, key=32B):
  エントリ = (4064 - sizeof(page_id_t)) / (32 + sizeof(page_id_t))
           = (4064 - 4) / (32 + 4) = 4060 / 36 ≈ 112 エントリ → ファンアウト 113

リーフノード (ID, key=32B, value=RecordId=6B):
  エントリ = 4064 / (32 + 6) = 4064 / 38 ≈ 106 エントリ

→ 100万件のイベントでもツリー高さ = ⌈log₁₁₃(1000000)⌉ = 3 (ルート→内部→リーフ)
→ わずか3回のページアクセスで任意のイベントに到達

Timeline (key=8B int64):
  内部: (4064-4)/(8+4) = 338 → ファンアウト 339
  リーフ: 4064/(8+6) = 290
  → 100万件でもツリー高さ2で到達
```

### 5.4 B+ツリー操作

#### 検索 (Search)

```text
btree_search(root_page, key):
  node = read_page(root_page)
  while node.is_leaf == false:
    i = binary_search(node.keys, key)
    node = read_page(node.children[i])
  i = binary_search(node.keys, key)
  if node.keys[i] == key:
    return node.values[i]
  return NOT_FOUND
```

#### 範囲スキャン (Range Scan)

```text
btree_range_scan(root_page, min_key, max_key, callback):
  // min_keyを持つリーフを検索
  leaf = find_leaf(root_page, min_key)
  while leaf != NULL:
    for each (key, value) in leaf:
      if key > max_key: return
      if key >= min_key: callback(key, value)
    leaf = read_page(leaf.right_sibling)
```

#### 挿入 (Insert) と分割 (Split)

```text
btree_insert(root_page, key, value):
  (leaf, ancestors) = find_leaf_with_path(root_page, key)

  if leaf has space:
    insert_into_leaf(leaf, key, value)
    return

  // リーフ分割
  (new_leaf, split_key) = split_leaf(leaf, key, value)

  // 分割キーを親に伝搬
  propagate_split(ancestors, split_key, new_leaf.page_id)

propagate_split(ancestors, key, right_child):
  while ancestors not empty:
    parent = ancestors.pop()
    if parent has space:
      insert_into_inner(parent, key, right_child)
      return
    (new_inner, split_key) = split_inner(parent, key, right_child)
    key = split_key
    right_child = new_inner.page_id
  // ルート分割: 新しいルートを作成
  new_root = create_inner_node([old_root, new_inner], [split_key])
  update_root(new_root.page_id)
```

#### 重複キーとオーバーフロー

kind/pubkey/tagインデックスでは同一キーに大量のRecordIdが紐づく。

**戦略**: リーフノードの値をRecordIdではなく「オーバーフローページチェーンの先頭page_id」とする。

```c
// オーバーフローページ (重複キー用)
typedef struct {
  SlotPageHeader header;        // page_type = PAGE_TYPE_OVERFLOW
  uint16_t       entry_count;   // このページ内のエントリ数
  page_id_t      next_overflow; // 次のオーバーフローページ (0 = 末端)
  uint8_t        reserved[2];
  // RecordId entries[...];     // RecordId配列 (created_at降順)
} OverflowPageHeader;

// 1オーバーフローページあたりの最大RecordIdエントリ数:
// (4096 - 32) / 6 ≈ 677 エントリ
```

### 5.5 API

```c
// B+ツリー ハンドル
typedef struct {
  BufferPool* pool;
  page_id_t   meta_page;       // メタデータページID
  BTreeMeta   meta;            // キャッシュ済みメタデータ
  BTreeKeyCompare compare;     // キー比較関数
} BTree;

// 初期化
NostrDBError btree_create(BTree* tree, BufferPool* pool, uint16_t key_size, uint16_t value_size, BTreeKeyType key_type);
NostrDBError btree_open(BTree* tree, BufferPool* pool, page_id_t meta_page);

// CRUD
NostrDBError btree_insert(BTree* tree, const void* key, const void* value);
NostrDBError btree_search(BTree* tree, const void* key, void* value_out);
NostrDBError btree_delete(BTree* tree, const void* key);

// 範囲スキャン
typedef bool (*BTreeScanCallback)(const void* key, const void* value, void* user_data);
NostrDBError btree_range_scan(BTree* tree, const void* min_key, const void* max_key, BTreeScanCallback callback, void* user_data);

// 重複キーインデックス用 (1キーに対してRecordIdリストを管理)
NostrDBError btree_insert_dup(BTree* tree, const void* key, RecordId rid);
NostrDBError btree_scan_key(BTree* tree, const void* key, BTreeScanCallback callback, void* user_data);
NostrDBError btree_delete_dup(BTree* tree, const void* key, RecordId rid);
```

### TODO Phase 5

- [x] **5-1**: `btree/btree_types.h` — `BTreeMeta`, `BTreeNodeHeader`, `BTreeKeyType`, キー比較関数型定義
- [x] **5-2**: `btree/btree.h` — B+ツリーAPI宣言
- [x] **5-3**: `btree/btree_node.c` — ノード操作ユーティリティ
    - `btree_node_init_leaf(PageData*, page_id_t)` — リーフノード初期化
    - `btree_node_init_inner(PageData*, page_id_t)` — 内部ノード初期化
    - `btree_node_search_key(PageData*, const void* key, BTreeKeyCompare)` — ノード内バイナリサーチ
    - `btree_node_insert_at(PageData*, uint16_t pos, const void* key, const void* value)` — 指定位置に挿入
    - `btree_node_key_at(const PageData*, uint16_t pos)` — 指定位置のキーポインタ取得
    - `btree_node_value_at(const PageData*, uint16_t pos)` — 指定位置の値ポインタ取得
    - `btree_node_child_at(const PageData*, uint16_t pos)` — 内部ノードの子ポインタ取得
- [x] **5-4**: `btree/btree_search.c` — 検索 / 範囲スキャン実装
    - `btree_search()` — ルートからリーフへの探索
    - `btree_range_scan()` — リーフ間リンクを辿る範囲スキャン
    - `btree_find_leaf()` — キーが所属するリーフノードを返す内部関数
- [x] **5-5**: `btree/btree_insert.c` — 挿入 / 分割実装
    - `btree_insert()` — リーフへの挿入
    - `btree_split_leaf()` — リーフ分割 (上位半分を新ノードに移動、右兄弟ポインタ繋ぎ替え)
    - `btree_split_inner()` — 内部ノード分割 (中央キーを親へ押し上げ)
    - `btree_grow_root()` — ルート分割時の新ルート作成
- [x] **5-6**: `btree/btree_delete.c` — 削除実装
    - `btree_delete()` — リーフからの削除
    - マージ/再分配は当面スキップ (トゥームストーン方式、定期的なリビルドで対応)
- [x] **5-7**: `btree/btree_overflow.c` — 重複キー用オーバーフローチェーン
    - `btree_insert_dup()` — オーバーフローページにRecordId追加
    - `btree_scan_key()` — オーバーフローチェーンを辿って全RecordIdを返す
    - `btree_delete_dup()` — オーバーフローチェーンからRecordId削除
- [x] **5-8**: `btree/btree_compare.c` — キー比較関数群
    - `btree_compare_bytes32()` — 32バイトmemcmp
    - `btree_compare_int64()` — int64 比較 (降順対応)
    - `btree_compare_uint32()` — uint32 比較
    - `btree_compare_composite_pk_kind()` — pubkey[32]+kind[4] 比較
    - `btree_compare_composite_tag()` — tag_name[1]+tag_value[32] 比較
- [x] **5-9**: `tests/btree-test.cpp` — B+ツリーの基本操作テスト
    - 挿入 (順次・逆順・ランダム)、検索、削除
    - 分割の発生確認 (大量挿入)
    - 範囲スキャン
    - 重複キーのオーバーフローチェーン

---

## Phase 6: インデックス統合 (`src/nostr/db/index/`)

汎用B+ツリーの上にNostr固有のインデックスロジックを構築する。

### 6.1 インデックス一覧

| インデックス | キー | 値 | B+ツリー種別 | 用途 |
|-------------|------|-----|-------------|------|
| **ID** | `id[32]` | `RecordId` | ユニーク | イベントID完全一致検索 |
| **Timeline** | `created_at` (int64, 降順) | `RecordId` | 重複許可 | 時間範囲スキャン、全件走査 |
| **Pubkey** | `pubkey[32]` | `RecordId` (overflow) | 重複許可 | author検索 |
| **Kind** | `kind` (uint32) | `RecordId` (overflow) | 重複許可 | kind検索 |
| **Pubkey+Kind** | `pubkey[32]+kind[4]` (36B) | `RecordId` (overflow) | 重複許可 | 複合検索 |
| **Tag** | `tag_name[1]+tag_value[32]` (33B) | `RecordId` (overflow) | 重複許可 | タグ検索 |

### 6.2 IDインデックスの選択肢

IDインデックスは完全一致のみで範囲検索不要なため、2つの選択肢がある:

**オプションA: B+ツリー** (統一性優先)

- 他のインデックスと同じコードパスで実装可能
- プレフィックスマッチ (Nostr仕様) にも対応可能

**オプションB: 拡張ハッシュ法** (性能優先)

- O(1)ルックアップ、B+ツリーの3ページアクセスに対し1-2ページアクセス
- IDはSHA-256ハッシュでありキー分布が均一

**決定**: 初期実装はB+ツリーで統一 (コード再利用)、プロファイリング後に必要であればIDのみ拡張ハッシュに置き換え。

### 6.3 Timelineインデックスの降順対応

`created_at` をB+ツリーのキーとして格納する際、`INT64_MAX - created_at` に変換して格納する。これにより、B+ツリーの自然な昇順スキャンが降順 (新しい順) になる。

```c
static inline int64_t timeline_key_encode(int64_t created_at) {
  return INT64_MAX - created_at;  // 新しいタイムスタンプ = 小さいキー = ツリーの左側
}

static inline int64_t timeline_key_decode(int64_t encoded) {
  return INT64_MAX - encoded;
}
```

### 6.4 インデックスマネージャ

```c
// 全インデックスの統合管理
typedef struct {
  BTree     id_index;
  BTree     timeline_index;
  BTree     pubkey_index;
  BTree     kind_index;
  BTree     pubkey_kind_index;
  BTree     tag_index;
  BufferPool* pool;
} IndexManager;

// 全インデックスへの一括操作
NostrDBError index_manager_insert_event(IndexManager* im, RecordId rid, const EventRecord* event);
NostrDBError index_manager_delete_event(IndexManager* im, RecordId rid, const EventRecord* event);
```

### TODO Phase 6

- [x] **6-1**: `index/index_manager.h` — `IndexManager` 構造体、API宣言
- [x] **6-2**: `index/index_id.c` — IDインデックス (B+ツリーラッパー)
    - `index_id_insert(BTree*, const uint8_t id[32], RecordId)` — ユニーク挿入
    - `index_id_lookup(BTree*, const uint8_t id[32], RecordId*)` — 完全一致検索
    - `index_id_prefix_scan(BTree*, const uint8_t* prefix, size_t len, callback)` — プレフィックスマッチ
    - `index_id_delete(BTree*, const uint8_t id[32])` — 削除
- [x] **6-3**: `index/index_timeline.c` — タイムラインインデックス
    - キーエンコード/デコード関数
    - `index_timeline_insert(BTree*, int64_t created_at, RecordId)`
    - `index_timeline_range(BTree*, int64_t since, int64_t until, uint32_t limit, callback)` — 範囲スキャン (降順)
- [x] **6-4**: `index/index_pubkey.c` — Pubkeyインデックス
    - `index_pubkey_insert(BTree*, const uint8_t pubkey[32], RecordId)`
    - `index_pubkey_scan(BTree*, const uint8_t pubkey[32], since, until, limit, callback)` — 特定pubkeyの全イベント
- [x] **6-5**: `index/index_kind.c` — Kindインデックス
    - `index_kind_insert(BTree*, uint32_t kind, RecordId)`
    - `index_kind_scan(BTree*, uint32_t kind, since, until, limit, callback)`
- [x] **6-6**: `index/index_pubkey_kind.c` — Pubkey+Kind複合インデックス
    - 複合キー構築 (`pubkey[32] || kind[4]`)
    - `index_pk_kind_insert(BTree*, const uint8_t pubkey[32], uint32_t kind, RecordId)`
    - `index_pk_kind_scan(BTree*, const uint8_t pubkey[32], uint32_t kind, since, until, limit, callback)`
- [x] **6-7**: `index/index_tag.c` — Tagインデックス
    - 複合キー構築 (`tag_name[1] || tag_value[32]`)
    - `index_tag_insert(BTree*, uint8_t name, const uint8_t value[32], RecordId)`
    - `index_tag_scan(BTree*, uint8_t name, const uint8_t value[32], since, until, limit, callback)`
- [x] **6-8**: `index/index_manager.c` — IndexManager初期化 / イベント挿入・削除時の全インデックス一括更新
- [x] **6-9**: `tests/index-test.cpp` — 各インデックスの単体テスト + 統合テスト

---

## Phase 7: クエリエンジン再構築 (`src/nostr/db/query/`)

現行のクエリエンジンをB+ツリーベースに書き換える。

### 7.1 クエリ戦略 (現行維持)

```text
優先度1: BY_ID        — idsフィルタ指定時 → IDインデックスで直接ルックアップ
優先度2: BY_TAG       — tagsフィルタ指定時 → Tagインデックスで範囲スキャン
優先度3: BY_PK_KIND   — authors+kinds指定時 → Pubkey+Kindインデックス
優先度4: BY_PUBKEY    — authors指定時 → Pubkeyインデックス
優先度5: BY_KIND      — kinds指定時 → Kindインデックス
優先度6: TIMELINE_SCAN — フォールバック → Timelineインデックスで全件スキャン
```

### 7.2 結果セット最適化

現行の `NostrDBResultSet` (動的mmapベース配列) を維持するが、重複チェックをO(N)線形スキャンからハッシュセットベースに改善。

```c
// 結果セット (改善版)
typedef struct {
  RecordId* rids;            // RecordId配列
  int64_t*  created_at;      // 並列配列 (ソート用)
  uint32_t  count;
  uint32_t  capacity;

  // 重複チェック用ブルームフィルタ (近似)
  uint64_t  bloom[64];       // 512バイト、offsetのハッシュでビットセット
} NostrDBResultSetV2;
```

### TODO Phase 7

- [x] **7-1**: `query/db_query_types.h` — `NostrDBFilter` は現行維持 (互換性)
- [x] **7-2**: `query/query_result_v2.c` — `QueryResultSet` をRecordIdベースに更新
    - ブルームフィルタによるO(1)近似重複チェック追加
- [x] **7-3**: `query/query_engine.c` — 各クエリ戦略をB+ツリーAPIに接続
    - `query_by_ids()` → `index_id_lookup()` に置き換え
    - `query_by_pubkey()` → `btree_scan_key(pubkey_index)` に置き換え
    - `query_by_kind()` → `btree_scan_key(kind_index)` に置き換え
    - `query_by_pubkey_kind()` → `btree_scan_key(pubkey_kind_index)` に置き換え
    - `query_by_tag()` → `btree_scan_key(tag_index)` に置き換え
    - `query_timeline_scan()` → `btree_range_scan(timeline_index)` に置き換え
- [x] **7-4**: `query/query_engine.c` — `query_post_filter()` のタグポストフィルタ実装
    - レコード読み出し → フィルタマッチング (ID, authors, kinds, time range)
- [x] **7-5**: `tests/query_engine_test.cpp` — 全クエリ戦略の統合テスト (14テスト)

---

## Phase 8: DB API統合 (`src/nostr/db/`)

上位レイヤーの公開APIを再構築し、既存コードとの互換性を維持する。

### 8.1 新NostrDB構造体

```c
struct NostrDB {
  DiskManager    disk;            // ディスクI/O
  BufferPool     buffer_pool;     // ページキャッシュ
  WalManager     wal;             // WAL
  IndexManager   indexes;         // 全インデックス

  // レコード管理
  page_id_t      record_meta_page; // レコードファイルのメタページ

  // 統計
  uint64_t       event_count;
  uint64_t       deleted_count;

  char           data_dir[256];
};
```

### 8.2 API互換性

公開APIのシグネチャは変更しない:

```c
// 既存API (変更なし)
NostrDBError nostr_db_init(NostrDB** db, const char* data_dir);
void         nostr_db_shutdown(NostrDB* db);
NostrDBError nostr_db_write_event(NostrDB* db, const NostrEventEntity* event);
NostrDBError nostr_db_get_event_by_id(NostrDB* db, const uint8_t* id, NostrEventEntity* out);
NostrDBError nostr_db_delete_event(NostrDB* db, const uint8_t* id);
NostrDBError nostr_db_get_event_at_offset(NostrDB* db, nostr_db_offset_t offset, NostrEventEntity* out);
NostrDBError nostr_db_query(NostrDB* db, const struct NostrFilter* filter, NostrDBResultSet* result);
NostrDBError nostr_db_get_stats(NostrDB* db, NostrDBStats* stats);
```

### 8.3 書き込みフロー (改善後)

```text
nostr_db_write_event(db, event):
  1. WAL: BEGIN
  2. event → EventRecord にシリアライズ
  3. RecordManager: レコード挿入 → RecordId 取得
  4. IndexManager: 全6インデックスに (キー, RecordId) 挿入
  5. WAL: 各ページ変更をUPDATEレコードとして記録
  6. WAL: COMMIT
  7. 統計カウンタ更新
  8. 一定間隔でチェックポイント
```

### 8.4 データファイル構成 (変更後)

```text
data/
├── nostr.db          # 単一データファイル (全ページをこの中に格納)
└── wal.log           # WALファイル
```

単一ファイルに統合することで:

- ファイルディスクリプタ消費を1本に削減 (現行7本)
- ページ割り当てを統一管理
- ファイルヘッダのページ0にインデックスルートページIDを格納

```c
// ファイルヘッダ拡張 (ページ0)
typedef struct {
  // 基本ヘッダ
  char       magic[8];              // "NOSTRDB2"
  uint32_t   version;               // 2
  uint32_t   page_size;
  uint32_t   total_pages;
  page_id_t  free_list_head;
  uint32_t   free_page_count;
  uint64_t   next_lsn;
  uint64_t   checkpoint_lsn;

  // イベントカウンタ
  uint64_t   event_count;
  uint64_t   deleted_count;

  // インデックスルートページ
  page_id_t  id_index_meta;         // IDインデックスメタページ
  page_id_t  timeline_index_meta;   // タイムラインインデックスメタページ
  page_id_t  pubkey_index_meta;     // Pubkeyインデックスメタページ
  page_id_t  kind_index_meta;       // Kindインデックスメタページ
  page_id_t  pk_kind_index_meta;    // Pubkey+Kindインデックスメタページ
  page_id_t  tag_index_meta;        // Tagインデックスメタページ

  // レコード管理
  page_id_t  first_record_page;     // 最初のレコードページ
  page_id_t  last_record_page;      // 最新のレコードページ (挿入先)

  uint8_t    reserved[...];         // 残りパディング
} FileHeaderV2;
```

### TODO Phase 8

- [x] **8-1**: `db_internal.h` — 新 `NostrDB` 構造体定義 (既存の `struct NostrDB` を置き換え)
- [x] **8-2**: `db_init.c` — 初期化フロー再実装
    - 単一ファイル `nostr.db` のオープン/作成
    - DiskManager → BufferPool → WalManager → IndexManager の順に初期化
    - WALリカバリ実行
- [x] **8-3**: `db_event.c` — `nostr_db_write_event` 再実装
    - EventRecordシリアライズ → RecordManager挿入 → IndexManager全インデックス更新
    - WALトランザクションでラップ
- [x] **8-4**: `db_event.c` — `nostr_db_get_event_by_id` 再実装
    - IDインデックス → RecordId → RecordManager読み出し → NostrEventEntityデシリアライズ
- [x] **8-5**: `db_event.c` — `nostr_db_get_event_at_offset` 再実装
    - RecordId (page_id + slot_index) から直接読み出し (offsetの意味がRecordIdに変更)
- [x] **8-6**: `db_event.c` — `nostr_db_delete_event` 再実装
    - IDインデックスでRecordId取得 → レコードの削除フラグ設定 → 各インデックスから削除
- [x] **8-7**: `db_init.c` — `nostr_db_shutdown` 再実装
    - チェックポイント実行 → バッファプールフラッシュ → 全リソース解放
- [x] **8-8**: `db_init.c` — `nostr_db_get_stats` 再実装
    - 各B+ツリーのentry_countを集計
- [ ] **8-9**: マイグレーション: 旧形式 (7ファイル) → 新形式 (単一ファイル) のデータ移行ユーティリティ (任意)
- [x] **8-10**: `tests/db-test.cpp` — 既存テストの更新 + 統合テスト
    - 書き込み → 読み出し → 削除 → クエリの一連フロー
    - 大量データ (10万件) の挿入・検索性能テスト

---

## Phase 9: テスト・ベンチマーク・最適化

### TODO Phase 9

- [x] **9-1**: `tests/CMakeLists.txt` — 新テスト追加 (disk-test, buffer-test, wal-test, btree-test, record-test, index-test, query-test)
- [x] **9-2**: クラッシュリカバリテスト
    - 書き込み途中でプロセスkill → 再起動 → データ整合性確認
- [x] **9-3**: 境界値テスト
    - ページ境界をまたぐレコード (スパンドレコード)
    - B+ツリー分割が連鎖するケース (大量挿入)
    - バッファプール満杯時のClock置換
- [x] **9-4**: ベンチマーク
    - 挿入スループット: 10万イベント/秒目標
    - 検索レイテンシ: ID検索 < 10μs (3ページアクセス)
    - 範囲スキャン: 1000件/ms目標
- [ ] **9-5**: プロファイリングと最適化
    - perf/valgring(callgrind) でホットパス特定
    - 必要に応じてIDインデックスを拡張ハッシュに差し替え
    - SIMD (SSE4.2) によるキー比較高速化の検討

---

## 実装順序とマイルストーン

```text
Phase 1: ディスク管理 (1-1 〜 1-6)
   ↓  ページ読み書きが動作
Phase 2: バッファプール (2-1 〜 2-7)
   ↓  ページキャッシュが動作
Phase 3: WAL (3-1 〜 3-8)
   ↓  クラッシュリカバリが動作
Phase 4: レコード管理 (4-1 〜 4-7)
   ↓  可変長イベントの読み書きが動作
Phase 5: B+ツリー (5-1 〜 5-9)
   ↓  汎用B+ツリーが動作 ← 最大の実装量
Phase 6: インデックス統合 (6-1 〜 6-9)
   ↓  Nostr固有インデックスが動作
Phase 7: クエリエンジン (7-1 〜 7-5)
   ↓  フィルタクエリが動作
Phase 8: DB API統合 (8-1 〜 8-10)
   ↓  既存コードからの呼び出しが動作
Phase 9: テスト・最適化 (9-1 〜 9-5)
   ↓  品質保証完了
```

### 推定ファイル構成 (新規)

```text
src/nostr/db/
├── disk/
│   ├── disk_types.h
│   ├── disk_manager.h
│   ├── disk_manager.c
│   ├── disk_io.c
│   └── disk_alloc.c
├── buffer/
│   ├── buffer_types.h
│   ├── buffer_pool.h
│   └── buffer_pool.c
├── wal/
│   ├── wal_types.h
│   ├── wal_manager.h
│   ├── wal_manager.c
│   ├── wal_write.c
│   ├── wal_flush.c
│   ├── wal_recovery.c
│   └── wal_checkpoint.c
├── record/
│   ├── record_types.h
│   ├── slot_page.h
│   ├── slot_page.c
│   ├── record_manager.h
│   ├── record_manager.c
│   ├── overflow.c
│   └── event_serializer.c
├── btree/
│   ├── btree_types.h
│   ├── btree.h
│   ├── btree_node.c
│   ├── btree_search.c
│   ├── btree_insert.c
│   ├── btree_delete.c
│   ├── btree_overflow.c
│   └── btree_compare.c
├── index/
│   ├── index_manager.h
│   ├── index_manager.c
│   ├── index_id.c
│   ├── index_timeline.c
│   ├── index_pubkey.c
│   ├── index_kind.c
│   ├── index_pubkey_kind.c
│   └── index_tag.c
├── query/
│   ├── db_query_types.h    (維持)
│   ├── db_query.h          (維持)
│   ├── db_query.c          (書き換え)
│   ├── db_query_result.h   (維持)
│   └── db_query_result.c   (書き換え)
├── db.h                    (API維持)
├── db_internal.h           (書き換え)
├── db_types.h              (維持+拡張)
├── db_init.c               (書き換え)
├── db_event.c              (書き換え)
└── db_tags.c               (維持)

tests/
├── disk-test.cpp
├── buffer-test.cpp
├── wal-test.cpp
├── btree-test.cpp
├── record-test.cpp
├── index-test.cpp
└── query-test.cpp
```

---

## 全TODOサマリ (62タスク)

| Phase | タスク数 | 概要 |
|-------|---------|------|
| 1. ディスク管理 | 6 | ページI/O、フリーリスト、ファイル拡張 |
| 2. バッファプール | 7 | SoAフレーム管理、Clock置換、ピン留め |
| 3. WAL | 8 | WALレコード、フラッシュ、リカバリ、チェックポイント |
| 4. レコード管理 | 7 | スロットページ、可変長レコード、オーバーフロー |
| 5. B+ツリー | 9 | ノード操作、検索、挿入、分割、削除、重複キー |
| 6. インデックス統合 | 9 | 6種インデックス、IndexManager |
| 7. クエリエンジン | 5 | 戦略実行、ポストフィルタ |
| 8. DB API統合 | 10 | NostrDB再構築、API接続、マイグレーション |
| 9. テスト・最適化 | 5 | リカバリ、境界値、ベンチマーク、プロファイリング |
| **合計** | **66** | |

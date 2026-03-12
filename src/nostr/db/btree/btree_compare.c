#include "../../../arch/memory.h"
#include "../../../util/string.h"
#include "btree.h"

// ============================================================================
// btree_compare_bytes32: Compare two 32-byte keys (memcmp)
// ============================================================================
int32_t btree_compare_bytes32(const void* a, const void* b, uint16_t key_size)
{
  (void)key_size;
  return internal_memcmp(a, b, 32);
}

// ============================================================================
// btree_compare_int64: Compare two int64_t keys
// ============================================================================
int32_t btree_compare_int64(const void* a, const void* b, uint16_t key_size)
{
  (void)key_size;
  int64_t va, vb;
  internal_memcpy(&va, a, sizeof(int64_t));
  internal_memcpy(&vb, b, sizeof(int64_t));
  if (va < vb) return -1;
  if (va > vb) return 1;
  return 0;
}

// ============================================================================
// btree_compare_uint32: Compare two uint32_t keys
// ============================================================================
int32_t btree_compare_uint32(const void* a, const void* b, uint16_t key_size)
{
  (void)key_size;
  uint32_t va, vb;
  internal_memcpy(&va, a, sizeof(uint32_t));
  internal_memcpy(&vb, b, sizeof(uint32_t));
  if (va < vb) return -1;
  if (va > vb) return 1;
  return 0;
}

// ============================================================================
// btree_compare_composite_pk_kind: Compare pubkey[32]+kind[4] = 36 bytes
// First compare pubkey (32 bytes), then kind (4 bytes) if equal
// ============================================================================
int32_t btree_compare_composite_pk_kind(const void* a, const void* b,
                                        uint16_t key_size)
{
  (void)key_size;
  int32_t cmp = internal_memcmp(a, b, 32);
  if (cmp != 0) return cmp;

  const uint8_t* pa = (const uint8_t*)a + 32;
  const uint8_t* pb = (const uint8_t*)b + 32;
  uint32_t       ka, kb;
  internal_memcpy(&ka, pa, sizeof(uint32_t));
  internal_memcpy(&kb, pb, sizeof(uint32_t));
  if (ka < kb) return -1;
  if (ka > kb) return 1;
  return 0;
}

// ============================================================================
// btree_compare_composite_tag: Compare tag_name[1]+tag_value[32] = 33 bytes
// First compare tag_name (1 byte), then tag_value (32 bytes) if equal
// ============================================================================
int32_t btree_compare_composite_tag(const void* a, const void* b,
                                    uint16_t key_size)
{
  (void)key_size;
  const uint8_t* pa = (const uint8_t*)a;
  const uint8_t* pb = (const uint8_t*)b;

  if (pa[0] < pb[0]) return -1;
  if (pa[0] > pb[0]) return 1;

  return internal_memcmp(pa + 1, pb + 1, 32);
}

// ============================================================================
// btree_get_comparator: Return the comparison function for a key type
// ============================================================================
BTreeKeyCompare btree_get_comparator(BTreeKeyType key_type)
{
  switch (key_type) {
    case BTREE_KEY_BYTES32:
      return btree_compare_bytes32;
    case BTREE_KEY_INT64:
      return btree_compare_int64;
    case BTREE_KEY_UINT32:
      return btree_compare_uint32;
    case BTREE_KEY_COMPOSITE:
      return btree_compare_composite_pk_kind;
    default:
      return btree_compare_bytes32;
  }
}

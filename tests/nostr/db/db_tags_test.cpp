#include <gtest/gtest.h>

#include <cstdint>
#include <cstring>

// Forward declare the C structures and functions to avoid header conflicts
// These must match the definitions in the actual C code

#define NOSTR_EVENT_TAG_VALUE_COUNT 16
#define NOSTR_EVENT_TAG_VALUE_LENGTH 512

extern "C" {

typedef struct {
  char   key[64];
  char   values[NOSTR_EVENT_TAG_VALUE_COUNT][NOSTR_EVENT_TAG_VALUE_LENGTH];
  size_t item_count;
} NostrTagEntity;

// NostrDBEventsHeader (64 bytes)
typedef struct {
  char     magic[8];
  uint32_t version;
  uint32_t flags;
  uint64_t event_count;
  uint64_t next_write_offset;
  uint64_t deleted_count;
  uint64_t file_size;
  uint8_t  reserved[16];
} NostrDBEventsHeader;

// NostrDBIndexHeader (64 bytes)
typedef struct {
  char     magic[8];
  uint32_t version;
  uint32_t flags;
  uint64_t bucket_count;
  uint64_t entry_count;
  uint64_t pool_next_offset;
  uint64_t pool_size;
  uint8_t  reserved[16];
} NostrDBIndexHeader;

// NostrDBEventHeader (48 bytes)
typedef struct {
  uint32_t total_length;
  uint32_t flags;
  uint8_t  id[32];
  int64_t  created_at;
} NostrDBEventHeader;

// Tag serialization functions
int64_t nostr_db_serialize_tags(
    const NostrTagEntity* tags,
    uint32_t tag_count,
    uint8_t* buffer,
    size_t capacity);

int32_t nostr_db_deserialize_tags(
    const uint8_t* buffer,
    size_t length,
    NostrTagEntity* tags,
    uint32_t max_tags);

}  // extern "C"

class NostrDBTagsTest : public ::testing::Test {
protected:
  void SetUp() override {
    memset(&tags, 0, sizeof(tags));
    memset(buffer, 0, sizeof(buffer));
  }

  NostrTagEntity tags[10];
  uint8_t buffer[4096];
};

TEST_F(NostrDBTagsTest, SerializeEmptyTags) {
  int64_t result = nostr_db_serialize_tags(tags, 0, buffer, sizeof(buffer));

  ASSERT_EQ(result, 2);  // Just the tag count (uint16_t)
  EXPECT_EQ(buffer[0], 0);  // tag_count low byte
  EXPECT_EQ(buffer[1], 0);  // tag_count high byte
}

TEST_F(NostrDBTagsTest, SerializeSingleTagSingleValue) {
  // Set up a single tag with one value
  strcpy(tags[0].key, "e");
  strcpy(tags[0].values[0], "abc123");
  tags[0].item_count = 1;

  int64_t result = nostr_db_serialize_tags(tags, 1, buffer, sizeof(buffer));

  ASSERT_GT(result, 0);

  // Verify: tag_count(2) + value_count(1) + name_len(1) + name(1) + value_len(2) + value(6)
  // = 2 + 1 + 1 + 1 + 2 + 6 = 13 bytes
  EXPECT_EQ(result, 13);

  // Deserialize and verify
  NostrTagEntity out_tags[10];
  memset(out_tags, 0, sizeof(out_tags));

  int32_t tag_count = nostr_db_deserialize_tags(buffer, (size_t)result, out_tags, 10);

  ASSERT_EQ(tag_count, 1);
  EXPECT_STREQ(out_tags[0].key, "e");
  EXPECT_EQ(out_tags[0].item_count, 1u);
  EXPECT_STREQ(out_tags[0].values[0], "abc123");
}

TEST_F(NostrDBTagsTest, SerializeMultipleTags) {
  // Set up two tags
  strcpy(tags[0].key, "e");
  strcpy(tags[0].values[0], "event_id_123");
  tags[0].item_count = 1;

  strcpy(tags[1].key, "p");
  strcpy(tags[1].values[0], "pubkey_456");
  strcpy(tags[1].values[1], "relay_url");
  tags[1].item_count = 2;

  int64_t result = nostr_db_serialize_tags(tags, 2, buffer, sizeof(buffer));

  ASSERT_GT(result, 0);

  // Deserialize and verify
  NostrTagEntity out_tags[10];
  memset(out_tags, 0, sizeof(out_tags));

  int32_t tag_count = nostr_db_deserialize_tags(buffer, (size_t)result, out_tags, 10);

  ASSERT_EQ(tag_count, 2);

  EXPECT_STREQ(out_tags[0].key, "e");
  EXPECT_EQ(out_tags[0].item_count, 1u);
  EXPECT_STREQ(out_tags[0].values[0], "event_id_123");

  EXPECT_STREQ(out_tags[1].key, "p");
  EXPECT_EQ(out_tags[1].item_count, 2u);
  EXPECT_STREQ(out_tags[1].values[0], "pubkey_456");
  EXPECT_STREQ(out_tags[1].values[1], "relay_url");
}

TEST_F(NostrDBTagsTest, SerializeNullBufferFails) {
  int64_t result = nostr_db_serialize_tags(tags, 1, nullptr, 100);
  EXPECT_EQ(result, -1);
}

TEST_F(NostrDBTagsTest, SerializeSmallCapacityFails) {
  strcpy(tags[0].key, "e");
  strcpy(tags[0].values[0], "value");
  tags[0].item_count = 1;

  // Buffer too small for even the tag count
  int64_t result = nostr_db_serialize_tags(tags, 1, buffer, 1);
  EXPECT_EQ(result, -1);
}

TEST_F(NostrDBTagsTest, DeserializeNullBufferFails) {
  int32_t result = nostr_db_deserialize_tags(nullptr, 100, tags, 10);
  EXPECT_EQ(result, -1);
}

TEST_F(NostrDBTagsTest, DeserializeNullTagsFails) {
  int32_t result = nostr_db_deserialize_tags(buffer, 100, nullptr, 10);
  EXPECT_EQ(result, -1);
}

TEST_F(NostrDBTagsTest, DeserializeSmallBufferFails) {
  int32_t result = nostr_db_deserialize_tags(buffer, 1, tags, 10);
  EXPECT_EQ(result, -1);
}

TEST_F(NostrDBTagsTest, RoundTripLongTagName) {
  // Test with a longer tag name
  strcpy(tags[0].key, "long_tag_name");
  strcpy(tags[0].values[0], "value1");
  strcpy(tags[0].values[1], "value2");
  tags[0].item_count = 2;

  int64_t ser_result = nostr_db_serialize_tags(tags, 1, buffer, sizeof(buffer));
  ASSERT_GT(ser_result, 0);

  NostrTagEntity out_tags[10];
  memset(out_tags, 0, sizeof(out_tags));

  int32_t tag_count = nostr_db_deserialize_tags(buffer, (size_t)ser_result, out_tags, 10);

  ASSERT_EQ(tag_count, 1);
  EXPECT_STREQ(out_tags[0].key, "long_tag_name");
  EXPECT_EQ(out_tags[0].item_count, 2u);
  EXPECT_STREQ(out_tags[0].values[0], "value1");
  EXPECT_STREQ(out_tags[0].values[1], "value2");
}

TEST_F(NostrDBTagsTest, RoundTripManyValues) {
  // Test with many values
  strcpy(tags[0].key, "t");
  for (int i = 0; i < 5; i++) {
    snprintf(tags[0].values[i], NOSTR_EVENT_TAG_VALUE_LENGTH, "hashtag_%d", i);
  }
  tags[0].item_count = 5;

  int64_t ser_result = nostr_db_serialize_tags(tags, 1, buffer, sizeof(buffer));
  ASSERT_GT(ser_result, 0);

  NostrTagEntity out_tags[10];
  memset(out_tags, 0, sizeof(out_tags));

  int32_t tag_count = nostr_db_deserialize_tags(buffer, (size_t)ser_result, out_tags, 10);

  ASSERT_EQ(tag_count, 1);
  EXPECT_STREQ(out_tags[0].key, "t");
  EXPECT_EQ(out_tags[0].item_count, 5u);
  for (int i = 0; i < 5; i++) {
    char expected[32];
    snprintf(expected, sizeof(expected), "hashtag_%d", i);
    EXPECT_STREQ(out_tags[0].values[i], expected);
  }
}

TEST_F(NostrDBTagsTest, StructSizes) {
  // Verify struct sizes as defined in TODO.md
  EXPECT_EQ(sizeof(NostrDBEventsHeader), 64u);
  EXPECT_EQ(sizeof(NostrDBIndexHeader), 64u);
  EXPECT_EQ(sizeof(NostrDBEventHeader), 48u);
}

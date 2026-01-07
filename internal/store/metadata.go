package store

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"path/filepath"

	"github.com/RoaringBitmap/roaring"
	"github.com/boltdb/bolt"
)

var (
	bucketSegments  = []byte("segments")
	bucketDeletions = []byte("deletions")
	bucketDocIDs    = []byte("docids")
	bucketMeta      = []byte("meta")
	keySegmentList  = []byte("list")
	keyEpoch        = []byte("epoch")
)

// DocMapping stores segment ID and docNum for an external ID.
type DocMapping struct {
	SegmentID string `json:"s"`
	DocNum    uint64 `json:"d"`
}

// Metadata provides persistent storage for index metadata using BoltDB.
type Metadata struct {
	db *bolt.DB
}

// NewMetadata opens or creates a metadata store.
func NewMetadata(dir string) (*Metadata, error) {
	dbPath := filepath.Join(dir, "meta.db")
	db, err := bolt.Open(dbPath, 0600, nil)
	if err != nil {
		return nil, err
	}

	// Initialize buckets
	err = db.Update(func(tx *bolt.Tx) error {
		for _, bucket := range [][]byte{bucketSegments, bucketDeletions, bucketDocIDs, bucketMeta} {
			if _, err := tx.CreateBucketIfNotExists(bucket); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		db.Close()
		return nil, err
	}

	return &Metadata{db: db}, nil
}

// GetSegments returns the list of segment IDs.
func (m *Metadata) GetSegments() ([]string, error) {
	var segments []string
	err := m.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketSegments)
		data := b.Get(keySegmentList)
		if data == nil {
			return nil
		}
		return json.Unmarshal(data, &segments)
	})
	return segments, err
}

// GetDeletions returns the deletion bitmap for a segment.
func (m *Metadata) GetDeletions(segmentID string) (*roaring.Bitmap, error) {
	bm := roaring.New()
	err := m.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketDeletions)
		data := b.Get([]byte(segmentID))
		if data == nil {
			return nil
		}
		_, err := bm.ReadFrom(bytes.NewReader(data))
		return err
	})
	return bm, err
}

// GetDocMapping returns the segment ID and docNum for an external ID.
func (m *Metadata) GetDocMapping(externalID string) (segmentID string, docNum uint64, found bool, err error) {
	var mapping DocMapping
	err = m.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketDocIDs)
		data := b.Get([]byte(externalID))
		if data == nil {
			return nil
		}
		found = true
		return json.Unmarshal(data, &mapping)
	})
	return mapping.SegmentID, mapping.DocNum, found, err
}

// GetEpoch returns the current epoch.
func (m *Metadata) GetEpoch() (uint64, error) {
	var epoch uint64
	err := m.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketMeta)
		data := b.Get(keyEpoch)
		if data == nil {
			return nil
		}
		epoch = binary.BigEndian.Uint64(data)
		return nil
	})
	return epoch, err
}

func (m *Metadata) Close() error {
	return m.db.Close()
}

// Update runs fn within a write transaction.
func (m *Metadata) Update(fn func(*Tx) error) error {
	return m.db.Update(func(tx *bolt.Tx) error {
		return fn(&Tx{tx: tx})
	})
}

// Tx provides write operations within a transaction.
type Tx struct {
	tx *bolt.Tx
}

// SetSegments sets the list of segment IDs.
func (t *Tx) SetSegments(segmentIDs []string) error {
	b := t.tx.Bucket(bucketSegments)
	data, err := json.Marshal(segmentIDs)
	if err != nil {
		return err
	}
	return b.Put(keySegmentList, data)
}

// SetDeletions sets the deletion bitmap for a segment.
func (t *Tx) SetDeletions(segmentID string, bm *roaring.Bitmap) error {
	b := t.tx.Bucket(bucketDeletions)
	var buf bytes.Buffer
	if _, err := bm.WriteTo(&buf); err != nil {
		return err
	}
	return b.Put([]byte(segmentID), buf.Bytes())
}

// GetDeletions returns the deletion bitmap for a segment.
func (t *Tx) GetDeletions(segmentID string) (*roaring.Bitmap, error) {
	bm := roaring.New()
	b := t.tx.Bucket(bucketDeletions)
	data := b.Get([]byte(segmentID))
	if data == nil {
		return bm, nil
	}
	_, err := bm.ReadFrom(bytes.NewReader(data))
	return bm, err
}

// DeleteDeletions removes the deletion bitmap for a segment.
func (t *Tx) DeleteDeletions(segmentID string) error {
	b := t.tx.Bucket(bucketDeletions)
	return b.Delete([]byte(segmentID))
}

// SetDocMapping sets the mapping from external ID to segment/docNum.
func (t *Tx) SetDocMapping(externalID, segmentID string, docNum uint64) error {
	b := t.tx.Bucket(bucketDocIDs)
	data, err := json.Marshal(DocMapping{SegmentID: segmentID, DocNum: docNum})
	if err != nil {
		return err
	}
	return b.Put([]byte(externalID), data)
}

// IncrementEpoch increments and returns the epoch.
func (t *Tx) IncrementEpoch() (uint64, error) {
	b := t.tx.Bucket(bucketMeta)
	var epoch uint64
	data := b.Get(keyEpoch)
	if data != nil {
		epoch = binary.BigEndian.Uint64(data)
	}
	epoch++
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, epoch)
	return epoch, b.Put(keyEpoch, buf)
}

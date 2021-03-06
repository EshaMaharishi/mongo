/**
 *    Copyright (C) 2014 MongoDB Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the GNU Affero General Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#pragma once

#include "mongo/db/catalog/index_catalog_entry.h"
#include "mongo/db/diskloc.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/structure/btree/btree_ondisk.h"
#include "mongo/db/structure/btree/key.h"
#include "mongo/db/structure/btree/bucket_deletion_notification.h"
#include "mongo/db/structure/head_manager.h"


namespace mongo {

    class BucketDeletionNotification;
    class RecordStore;

    // Used for unit-testing only
    template <class BtreeLayout> class BtreeLogicTestBase;
    template <class BtreeLayout> class ArtificialTreeBuilder;

    /**
     * This is the logic for manipulating the Btree.  It is (mostly) independent of the on-disk
     * format.
     */
    template <class BtreeLayout>
    class BtreeLogic {
    public:
        // AKA _keyNode
        typedef typename BtreeLayout::FixedWidthKeyType KeyHeaderType;

        // AKA Key
        typedef typename BtreeLayout::KeyType KeyDataType;

        // AKA KeyOwned
        typedef typename BtreeLayout::KeyOwnedType KeyDataOwnedType;

        // AKA Loc
        typedef typename BtreeLayout::LocType LocType;

        // AKA BucketBasics or BtreeBucket, either one.
        typedef typename BtreeLayout::BucketType BucketType;

        /**
         * 'head' manages the catalog information.
         * 'store' allocates and frees buckets.
         * 'ordering' is meta-information we store in the catalog.
         * 'indexName' is a string identifying the index that we use to print errors with.
         */
        BtreeLogic(HeadManager* head,
                   RecordStore* store,
                   const Ordering& ordering,
                   const string& indexName,
                   BucketDeletionNotification* bucketDeletion)
            : _headManager(head),
              _recordStore(store),
              _ordering(ordering),
              _indexName(indexName),
              _bucketDeletion(bucketDeletion) { 
        
        }

        //
        // Public-facing
        //

        class Builder {
        public:
            typedef typename BtreeLayout::KeyOwnedType KeyDataOwnedType;
            typedef typename BtreeLayout::KeyType KeyDataType;

            Status addKey(const BSONObj& key, const DiskLoc& loc);

            // XXX: status, outparam for # keys?
            unsigned long long commit(bool mayInterrupt);

        private:
            friend class BtreeLogic;

            Builder(BtreeLogic* logic, OperationContext* txn, bool dupsAllowed);

            // Direct ports of functionality
            void newBucket();
            void buildNextLevel(DiskLoc loc, bool mayInterrupt);
            void mayCommitProgressDurably();
            BucketType* _getModifiableBucket(DiskLoc loc);
            BucketType* _getBucket(DiskLoc loc);
            // Direct ports of functionality

            // Not owned.
            BtreeLogic* _logic;

            // Direct port of names.
            DiskLoc _cur;
            DiskLoc _first;
            BucketType* _b;
            bool _committed;
            bool _dupsAllowed;
            long long _numAdded;
            auto_ptr<KeyDataOwnedType> _keyLast;

            // Not owned.
            OperationContext* _txn;
        };

        /**
         * Caller owns the returned pointer.
         * 'this' must outlive the returned pointer.
         */
        Builder* newBuilder(OperationContext* txn, bool dupsAllowed);

        Status dupKeyCheck(const BSONObj& key, const DiskLoc& loc) const;

        Status insert(OperationContext* txn,
                      const BSONObj& rawKey,
                      const DiskLoc& value,
                      bool dupsAllowed);

        /**
         * Navigates down the tree and locates the bucket and position containing a record with
         * the specified <key, recordLoc> combination.
         *
         * @return true if the exact <key, recordLoc> was found. Otherwise, false and the
         *      bucketLocOut would contain the bucket containing key which is before or after the
         *      searched one (dependent on the direction).
         */
        bool locate(const BSONObj& key,
                    const DiskLoc& recordLoc,
                    const int direction,
                    int* posOut,
                    DiskLoc* bucketLocOut) const;

        void advance(DiskLoc* bucketLocInOut, int* posInOut, int direction) const;

        bool exists(const KeyDataType& key) const;

        bool unindex(OperationContext* txn,
                     const BSONObj& key,
                     const DiskLoc& recordLoc);

        bool isEmpty() const;

        long long fullValidate(long long *unusedCount,
                               bool strict,
                               bool dumpBuckets,
                               unsigned depth);

        DiskLoc getDiskLoc(const DiskLoc& bucketLoc, const int keyOffset) const;

        BSONObj getKey(const DiskLoc& bucketLoc, const int keyOffset) const;

        DiskLoc getHead() const { return _headManager->getHead(); }

        Status touch(OperationContext* txn) const;

        //
        // Composite key navigation methods
        //

        void customLocate(DiskLoc* locInOut,
                          int* keyOfsInOut,
                          const BSONObj& keyBegin,
                          int keyBeginLen,
                          bool afterKey,
                          const vector<const BSONElement*>& keyEnd,
                          const vector<bool>& keyEndInclusive,
                          int direction) const;

        void advanceTo(DiskLoc* thisLocInOut,
                       int* keyOfsInOut,
                       const BSONObj &keyBegin,
                       int keyBeginLen,
                       bool afterKey,
                       const vector<const BSONElement*>& keyEnd,
                       const vector<bool>& keyEndInclusive,
                       int direction) const;

        void restorePosition(const BSONObj& savedKey,
                             const DiskLoc& savedLoc,
                             int direction,
                             DiskLoc* bucketInOut,
                             int* keyOffsetInOut) const;

        //
        // Creation and deletion
        //

        /**
         * Returns OK if the index was uninitialized before, error status otherwise.
         */
        Status initAsEmpty(OperationContext* txn);

        //
        // Size constants
        //

        static int lowWaterMark();

    private:
        friend class BtreeLogic::Builder;

        // Used for unit-testing only
        friend class BtreeLogicTestBase<BtreeLayout>;
        friend class ArtificialTreeBuilder<BtreeLayout>;

        /**
         * This is an in memory wrapper for the variable length data associated with a
         * KeyHeaderType.  It points to on-disk data but is not itself on-disk data.
         *
         * This object and its BSONObj 'key' will become invalid if the KeyHeaderType data that owns
         * this it is moved within the btree.  In general, a KeyWrapper should not be expected to be
         * valid after a write.
         */
        struct FullKey {
            FullKey(const BucketType* bucket, int i)
                : header(getKeyHeader(bucket, i)),
                  prevChildBucket(header.prevChildBucket),
                  recordLoc(header.recordLoc),
                  data(bucket->data + header.keyDataOfs()) { }

            // This is actually a reference to something on-disk.
            const KeyHeaderType& header;

            // These are actually in 'header'.
            const LocType& prevChildBucket;
            const LocType& recordLoc;

            // This is *not* memory-mapped but its members point to something on-disk.
            KeyDataType data;
        };

        //
        // Functions that depend on the templated type info but nothing in 'this'.
        //

        static LocType& childLocForPos(BucketType* bucket, int pos);

        static FullKey getFullKey(const BucketType* bucket, int i);

        static KeyHeaderType& getKeyHeader(BucketType* bucket, int i);

        static const KeyHeaderType& getKeyHeader(const BucketType* bucket, int i);

        static char* dataAt(BucketType* bucket, short ofs);

        static void markUnused(BucketType* bucket, int keypos);

        static int totalDataSize(BucketType* bucket);

        static void init(BucketType* bucket);

        static int _alloc(BucketType* bucket, int bytes);

        static void _unalloc(BucketType* bucket, int bytes);

        static void _delKeyAtPos(BucketType* bucket, int keypos, bool mayEmpty = false);

        static void popBack(BucketType* bucket, DiskLoc* recordLocOut, KeyDataType *keyDataOut);

        static bool mayDropKey(BucketType* bucket, int index, int refPos);

        static int _packedDataSize(BucketType* bucket, int refPos);

        static void setPacked(BucketType* bucket);

        static void setNotPacked(BucketType* bucket);

        static BucketType* btreemod(OperationContext* txn, BucketType* bucket);

        static int splitPos(BucketType* bucket, int keypos);

        static void reserveKeysFront(BucketType* bucket, int nAdd);

        static void setKey(BucketType* bucket,
                           int i,
                           const DiskLoc recordLoc,
                           const KeyDataType &key,
                           const DiskLoc prevChildBucket);

        static bool isHead(BucketType* bucket);

        static void dumpBucket(const BucketType* bucket, int indentLength = 0);

        static void assertValid(const std::string& ns, 
                                BucketType* bucket, 
                                const Ordering& ordering, 
                                bool force = false);

        //
        // 'this'-specific helpers (require record store, catalog information, or ordering, or type
        // information).
        //

        bool basicInsert(OperationContext* txn,
                         BucketType* bucket,
                         const DiskLoc bucketLoc,
                         int& keypos,
                         const KeyDataType& key,
                         const DiskLoc recordLoc);

        void dropFront(BucketType* bucket, int nDrop, int& refpos);

        void _pack(OperationContext* txn, BucketType* bucket, const DiskLoc thisLoc, int &refPos);

        void customLocate(DiskLoc* locInOut,
                          int* keyOfsInOut,
                          const BSONObj& keyBegin,
                          int keyBeginLen,
                          bool afterKey,
                          const vector<const BSONElement*>& keyEnd,
                          const vector<bool>& keyEndInclusive,
                          int direction,
                          pair<DiskLoc, int>& bestParent) const;

        Status _find(BucketType* bucket,
                    const KeyDataType& key,
                    const DiskLoc& recordLoc,
                    bool errorIfDup,
                    int* keyPositionOut,
                    bool* foundOut) const;

        bool customFind(int low,
                        int high,
                        const BSONObj& keyBegin,
                        int keyBeginLen,
                        bool afterKey,
                        const vector<const BSONElement*>& keyEnd,
                        const vector<bool>& keyEndInclusive,
                        const Ordering& order,
                        int direction,
                        DiskLoc* thisLocInOut,
                        int* keyOfsInOut,
                        pair<DiskLoc, int>& bestParent) const;

        void advanceToImpl(DiskLoc* thisLocInOut,
                           int* keyOfsInOut,
                           const BSONObj &keyBegin,
                           int keyBeginLen,
                           bool afterKey,
                           const vector<const BSONElement*>& keyEnd,
                           const vector<bool>& keyEndInclusive,
                           int direction) const;

        bool wouldCreateDup(const KeyDataType& key, const DiskLoc self) const;

        bool keyIsUsed(const DiskLoc& loc, const int& pos) const;

        void skipUnusedKeys(DiskLoc* loc, int* pos, int direction) const;

        DiskLoc advance(const DiskLoc& bucketLoc, int* posInOut, int direction) const;

        DiskLoc _locate(const DiskLoc& bucketLoc,
                       const KeyDataType& key,
                       int* posOut,
                       bool* foundOut,
                       const DiskLoc& recordLoc,
                       const int direction) const;

        long long _fullValidate(const DiskLoc bucketLoc,
                               long long *unusedCount,
                               bool strict,
                               bool dumpBuckets,
                               unsigned depth);

        DiskLoc _addBucket(OperationContext* txn);

        bool canMergeChildren(BucketType* bucket,
                              const DiskLoc bucketLoc,
                              const int leftIndex);

        // has to look in children of 'bucket' and requires record store
        int _rebalancedSeparatorPos(BucketType* bucket, int leftIndex);

        void _packReadyForMod(BucketType* bucket, int &refPos);

        void truncateTo(BucketType* bucket, int N, int &refPos);

        void split(OperationContext* txn,
                   BucketType* bucket,
                   const DiskLoc bucketLoc,
                   int keypos,
                   const DiskLoc recordLoc,
                   const KeyDataType& key,
                   const DiskLoc lchild,
                   const DiskLoc rchild);

        Status _insert(OperationContext* txn,
                       BucketType* bucket,
                       const DiskLoc bucketLoc,
                       const KeyDataType& key,
                       const DiskLoc recordLoc,
                       bool dupsAllowed,
                       const DiskLoc leftChild,
                       const DiskLoc rightChild);

        // TODO take a BucketType*?
        void insertHere(OperationContext* txn,
                        const DiskLoc bucketLoc,
                        int pos,
                        const KeyDataType& key,
                        const DiskLoc recordLoc,
                        const DiskLoc leftChild,
                        const DiskLoc rightChild);

        std::string dupKeyError(const KeyDataType& key) const;

        void setInternalKey(OperationContext* txn,
                            BucketType* bucket,
                            const DiskLoc bucketLoc,
                            int keypos,
                            const DiskLoc recordLoc,
                            const KeyDataType& key,
                            const DiskLoc lchild,
                            const DiskLoc rchild);

        void fixParentPtrs(OperationContext* trans,
                           BucketType* bucket,
                           const DiskLoc bucketLoc,
                           int firstIndex = 0,
                           int lastIndex = -1);

        bool mayBalanceWithNeighbors(OperationContext* txn, BucketType* bucket, const DiskLoc bucketLoc);

        void doBalanceChildren(OperationContext* txn,
                               BucketType* bucket,
                               const DiskLoc bucketLoc,
                               int leftIndex);

        void doBalanceLeftToRight(OperationContext* txn,
                                  BucketType* bucket,
                                  const DiskLoc thisLoc,
                                  int leftIndex,
                                  int split,
                                  BucketType* l,
                                  const DiskLoc lchild,
                                  BucketType* r,
                                  const DiskLoc rchild);

        void doBalanceRightToLeft(OperationContext* txn,
                                  BucketType* bucket,
                                  const DiskLoc bucketLoc,
                                  int leftIndex,
                                  int split,
                                  BucketType* l,
                                  const DiskLoc lchild,
                                  BucketType* r,
                                  const DiskLoc rchild);

        bool tryBalanceChildren(OperationContext* txn,
                                BucketType* bucket,
                                const DiskLoc bucketLoc,
                                int leftIndex);

        int indexInParent(BucketType* bucket, const DiskLoc bucketLoc) const;

        void doMergeChildren(OperationContext* txn,
                             BucketType* bucket,
                             const DiskLoc bucketLoc,
                             int leftIndex);

        void replaceWithNextChild(OperationContext* txn,
                                  BucketType* bucket,
                                  const DiskLoc bucketLoc);

        void deleteInternalKey(OperationContext* txn,
                               BucketType* bucket,
                               const DiskLoc bucketLoc,
                               int keypos);

        void delKeyAtPos(OperationContext* txn,
                         BucketType* bucket,
                         const DiskLoc bucketLoc,
                         int p);

        void delBucket(OperationContext* txn,
                       BucketType* bucket,
                       const DiskLoc bucketLoc);

        void deallocBucket(OperationContext* txn,
                           BucketType* bucket,
                           const DiskLoc bucketLoc);

        bool _keyIsAt(const BSONObj& savedKey,
                     const DiskLoc& savedLoc,
                     BucketType* bucket,
                     int keyPos) const;

        // TODO 'this' for _ordering(?)
        int customBSONCmp(const BSONObj& l,
                          const BSONObj& rBegin,
                          int rBeginLen,
                          bool rSup,
                          const std::vector<const BSONElement*>& rEnd,
                          const std::vector<bool>& rEndInclusive,
                          const Ordering& o,
                          int direction) const;

        // TODO needs 'this' for _ordering for sanity check
        bool _pushBack(BucketType* bucket,
                       const DiskLoc recordLoc,
                       const KeyDataType& key,
                       const DiskLoc prevChild);

        void pushBack(BucketType* bucket,
                      const DiskLoc recordLoc,
                      const KeyDataType& key,
                      const DiskLoc prevChild) {
            invariant(_pushBack(bucket, recordLoc, key, prevChild));
        }

        BucketType* childForPos(BucketType* bucket, int pos) const;

        BucketType* getBucket(const DiskLoc dl) const;

        BucketType* getRoot() const;

        DiskLoc getRootLoc() const;

        //
        // Data
        //

        // Not owned here.
        HeadManager* _headManager;

        // Not owned here.
        RecordStore* _recordStore;

        Ordering _ordering;

        string _indexName;

        // Not owned here
        BucketDeletionNotification* _bucketDeletion;
    };

}  // namespace mongo

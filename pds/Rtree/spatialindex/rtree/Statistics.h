// Spatial Index Library
//
// Copyright (C) 2002 Navel Ltd.
//
// This library is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public
// License as published by the Free Software Foundation; either
// version 2.1 of the License, or (at your option) any later version.
//
// This library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
// Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public
// License along with this library; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
//
//  Email:
//    mhadji@gmail.com

#ifndef __spatialindex_rtree_statistics_h
#define __spatialindex_rtree_statistics_h

namespace SpatialIndex
{
	namespace RTree
	{
		class RTree;
		class Node;
		class Leaf;
		class Index;

		class Statistics : public SpatialIndex::IStatistics
		{
		public:
			Statistics();
			Statistics(const Statistics&);
			virtual ~Statistics();
			Statistics& operator=(const Statistics&);

			//
			// IStatistics interface
			//
			virtual size_t getReads() const;
			virtual size_t getWrites() const;
			virtual size_t getNumberOfNodes() const;
			virtual size_t getNumberOfData() const;

			virtual size_t getSplits() const;
			virtual size_t getHits() const;
			virtual size_t getMisses() const;
			virtual size_t getAdjustments() const;
			virtual size_t getQueryResults() const;
			virtual size_t getTreeHeight() const;
			virtual size_t getNumberOfNodesInLevel(size_t l) const;

		private:
			void reset();

			size_t m_reads;

			size_t m_writes;

			size_t m_splits;

			size_t m_hits;

			size_t m_misses;

			size_t m_nodes;

			size_t m_adjustments;

			size_t m_queryResults;

			size_t m_data;

			size_t m_treeHeight;

			std::vector<size_t> m_nodesInLevel;

			friend class RTree;
			friend class Node;
			friend class Index;
			friend class Leaf;
			friend class BulkLoader;

			friend std::ostream& operator<<(std::ostream& os, const Statistics& s);
		}; // Statistics

		std::ostream& operator<<(std::ostream& os, const Statistics& s);
	}
}

#endif /*__spatialindex_rtree_statistics_h*/

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

#include <fstream>

#include "../spatialindex/SpatialIndexImpl.h"
#include "DiskStorageManager.h"

using namespace SpatialIndex;
using namespace SpatialIndex::StorageManager;

SpatialIndex::IStorageManager* SpatialIndex::StorageManager::returnDiskStorageManager(Tools::PropertySet& ps)
{
	IStorageManager* sm = new DiskStorageManager(ps);
	return sm;
}

SpatialIndex::IStorageManager* SpatialIndex::StorageManager::createNewDiskStorageManager(std::string& baseName, size_t pageSize)
{
	Tools::Variant var;
	Tools::PropertySet ps;

	var.m_varType = Tools::VT_BOOL;
	var.m_val.blVal = true;
	ps.setProperty("Overwrite", var);
		// overwrite the file if it exists.

	var.m_varType = Tools::VT_PCHAR;
	var.m_val.pcVal = const_cast<char*>(baseName.c_str());
	ps.setProperty("FileName", var);
		// .idx and .dat extensions will be added.

	var.m_varType = Tools::VT_ULONG;
	var.m_val.ulVal = pageSize;
	ps.setProperty("PageSize", var);
		// specify the page size. Since the index may also contain user defined data
		// there is no way to know how big a single node may become. The storage manager
		// will use multiple pages per node if needed. Off course this will slow down performance.

	return returnDiskStorageManager(ps);
}

SpatialIndex::IStorageManager* SpatialIndex::StorageManager::loadDiskStorageManager(std::string& baseName)
{
	Tools::Variant var;
	Tools::PropertySet ps;

	var.m_varType = Tools::VT_PCHAR;
	var.m_val.pcVal = const_cast<char*>(baseName.c_str());
	ps.setProperty("FileName", var);
		// .idx and .dat extensions will be added.

	return returnDiskStorageManager(ps);
}

DiskStorageManager::DiskStorageManager(Tools::PropertySet& ps) : m_pageSize(0), m_nextPage(-1), m_buffer(0)
{
	Tools::Variant var;

	// Open/Create flag.
	bool bOverwrite = false;
	var = ps.getProperty("Overwrite");

	if (var.m_varType != Tools::VT_EMPTY)
	{
		if (var.m_varType != Tools::VT_BOOL)
			throw Tools::IllegalArgumentException("SpatialIndex::DiskStorageManager: Property Overwrite must be Tools::VT_BOOL");
		bOverwrite = var.m_val.blVal;
	}

	// storage filename.
	var = ps.getProperty("FileName");

	if (var.m_varType != Tools::VT_EMPTY)
	{
		if (var.m_varType != Tools::VT_PCHAR)
			throw Tools::IllegalArgumentException("SpatialIndex::DiskStorageManager: Property FileName must be Tools::VT_PCHAR");

		size_t cLen = strlen(var.m_val.pcVal);

		std::string sIndexFile = std::string(var.m_val.pcVal) + ".idx";
		std::string sDataFile = std::string(var.m_val.pcVal) + ".dat";

		// check if file exists.
		bool bFileExists = true;
		std::ifstream fin1(sIndexFile.c_str(), std::ios::in | std::ios::binary);
		std::ifstream fin2(sDataFile.c_str(), std::ios::in | std::ios::binary);
		if (fin1.fail() || fin2.fail()) bFileExists = false;
		fin1.close(); fin2.close();

		// check if file can be read/written.
		if (bFileExists == true && bOverwrite == false)
		{
			m_indexFile.open(sIndexFile.c_str(), std::ios::in | std::ios::out | std::ios::binary);
			m_dataFile.open(sDataFile.c_str(), std::ios::in | std::ios::out | std::ios::binary);

			if (m_indexFile.fail() || m_dataFile.fail())
				throw Tools::IllegalArgumentException("SpatialIndex::DiskStorageManager: Index/Data file cannot be read/writen.");
		}
		else
		{
			m_indexFile.open(sIndexFile.c_str(), std::ios::in | std::ios::out | std::ios::binary | std::ios::trunc);
			m_dataFile.open(sDataFile.c_str(), std::ios::in | std::ios::out | std::ios::binary | std::ios::trunc);

			if (m_indexFile.fail() || m_dataFile.fail())
				throw Tools::IllegalArgumentException("SpatialIndex::DiskStorageManager: Index/Data file cannot be created.");

		}
	}
	else
	{
		throw Tools::IllegalArgumentException("SpatialIndex::DiskStorageManager: Property FileName was not specified.");
	}

	// find page size.
	if (bOverwrite == true)
	{
		var = ps.getProperty("PageSize");

		if (var.m_varType != Tools::VT_EMPTY)
		{
			if (var.m_varType != Tools::VT_ULONG)
				throw Tools::IllegalArgumentException("SpatialIndex::DiskStorageManager: Property PageSize must be Tools::VT_ULONG");
			m_pageSize = var.m_val.ulVal;
			m_nextPage = 0;
		}
		else
		{
			throw Tools::IllegalArgumentException("SpatialIndex::DiskStorageManager: A new storage manager is created and property PageSize was not specified.");
		}
	}
	else
	{
		m_indexFile.read(reinterpret_cast<char*>(&m_pageSize), sizeof(size_t));
		if (m_indexFile.fail())
			throw Tools::IllegalStateException("SpatialIndex::DiskStorageManager: Failed reading pageSize.");

		m_indexFile.read(reinterpret_cast<char*>(&m_nextPage), sizeof(id_type));
		if (m_indexFile.fail())
			throw Tools::IllegalStateException("SpatialIndex::DiskStorageManager: Failed reading nextPage.");
	}

	// create buffer.
	m_buffer = new byte[m_pageSize];
	bzero(m_buffer, m_pageSize);

	if (bOverwrite == false)
	{
		size_t count;
		id_type id, page;

		// load empty pages in memory.
		m_indexFile.read(reinterpret_cast<char*>(&count), sizeof(size_t));
		if (m_indexFile.fail())
			throw Tools::IllegalStateException("SpatialIndex::DiskStorageManager: Corrupted storage manager index file.");

		for (size_t cCount = 0; cCount < count; ++cCount)
		{
			m_indexFile.read(reinterpret_cast<char*>(&page), sizeof(id_type));
			if (m_indexFile.fail())
				throw Tools::IllegalStateException("SpatialIndex::DiskStorageManager: Corrupted storage manager index file.");
			m_emptyPages.push(page);
		}

		// load index table in memory.
		m_indexFile.read(reinterpret_cast<char*>(&count), sizeof(size_t));
		if (m_indexFile.fail())
			throw Tools::IllegalStateException("SpatialIndex::DiskStorageManager: Corrupted storage manager index file.");

		for (size_t cCount = 0; cCount < count; ++cCount)
		{
			Entry* e = new Entry();

			m_indexFile.read(reinterpret_cast<char*>(&id), sizeof(id_type));
			if (m_indexFile.fail())
				throw Tools::IllegalStateException("SpatialIndex::DiskStorageManager: Corrupted storage manager index file.");

			m_indexFile.read(reinterpret_cast<char*>(&(e->m_length)), sizeof(size_t));
			if (m_indexFile.fail())
				throw Tools::IllegalStateException("SpatialIndex::DiskStorageManager: Corrupted storage manager index file.");

			size_t count2;
			m_indexFile.read(reinterpret_cast<char*>(&count2), sizeof(size_t));
			if (m_indexFile.fail())
				throw Tools::IllegalStateException("SpatialIndex::DiskStorageManager: Corrupted storage manager index file.");

			for (size_t cCount2 = 0; cCount2 < count2; ++cCount2)
			{
				m_indexFile.read(reinterpret_cast<char*>(&page), sizeof(id_type));
				if (m_indexFile.fail())
					throw Tools::IllegalStateException("SpatialIndex::DiskStorageManager: Corrupted storage manager index file.");
				e->m_pages.push_back(page);
			}
			m_pageIndex.insert(std::pair<id_type, Entry* >(id, e));
		}
	}
}

DiskStorageManager::~DiskStorageManager()
{
	flush();
	m_indexFile.close();
	m_dataFile.close();
	if (m_buffer != 0) delete[] m_buffer;

	std::map<id_type, Entry*>::iterator it;
	for (it = m_pageIndex.begin(); it != m_pageIndex.end(); ++it) delete (*it).second;
}

void DiskStorageManager::flush()
{
	m_indexFile.seekp(0, std::ios_base::beg);
	if (m_indexFile.fail())
		throw Tools::IllegalStateException("SpatialIndex::DiskStorageManager: Corrupted storage manager index file.");

	m_indexFile.write(reinterpret_cast<const char*>(&m_pageSize), sizeof(size_t));
	if (m_indexFile.fail())
		throw Tools::IllegalStateException("SpatialIndex::DiskStorageManager: Corrupted storage manager index file.");

	m_indexFile.write(reinterpret_cast<const char*>(&m_nextPage), sizeof(id_type));
	if (m_indexFile.fail())
		throw Tools::IllegalStateException("SpatialIndex::DiskStorageManager: Corrupted storage manager index file.");

	size_t count = m_emptyPages.size();
	id_type id, page;

	m_indexFile.write(reinterpret_cast<const char*>(&count), sizeof(size_t));
	if (m_indexFile.fail())
			throw Tools::IllegalStateException("SpatialIndex::DiskStorageManager: Corrupted storage manager index file.");

	while (! m_emptyPages.empty())
	{
		page = m_emptyPages.top(); m_emptyPages.pop();
		m_indexFile.write(reinterpret_cast<const char*>(&page), sizeof(id_type));
		if (m_indexFile.fail())
			throw Tools::IllegalStateException("SpatialIndex::DiskStorageManager: Corrupted storage manager index file.");
	}

	count = m_pageIndex.size();

	m_indexFile.write(reinterpret_cast<const char*>(&count), sizeof(size_t));
	if (m_indexFile.fail())
		throw Tools::IllegalStateException("SpatialIndex::DiskStorageManager: Corrupted storage manager index file.");

	std::map<id_type, Entry*>::iterator it;

	for (it = m_pageIndex.begin(); it != m_pageIndex.end(); ++it)
	{
		id = (*it).first;
		m_indexFile.write(reinterpret_cast<const char*>(&id), sizeof(id_type));
		if (m_indexFile.fail())
			throw Tools::IllegalStateException("SpatialIndex::DiskStorageManager: Corrupted storage manager index file.");

		size_t length = (*it).second->m_length;
		m_indexFile.write(reinterpret_cast<const char*>(&length), sizeof(size_t));
		if (m_indexFile.fail())
			throw Tools::IllegalStateException("SpatialIndex::DiskStorageManager: Corrupted storage manager index file.");

		count = (*it).second->m_pages.size();
		m_indexFile.write(reinterpret_cast<const char*>(&count), sizeof(size_t));
		if (m_indexFile.fail())
			throw Tools::IllegalStateException("SpatialIndex::DiskStorageManager: Corrupted storage manager index file.");

		for (size_t cIndex = 0; cIndex < count; ++cIndex)
		{
			page = (*it).second->m_pages[cIndex];
			m_indexFile.write(reinterpret_cast<const char*>(&page), sizeof(id_type));
			if (m_indexFile.fail())
				throw Tools::IllegalStateException("SpatialIndex::DiskStorageManager: Corrupted storage manager index file.");
		}
	}

	m_indexFile.flush();
	m_dataFile.flush();
}

void DiskStorageManager::loadByteArray(const id_type id, size_t& len, byte** data)
{
	std::map<id_type, Entry*>::iterator it = m_pageIndex.find(id);

	if (it == m_pageIndex.end())
		throw Tools::InvalidPageException(id);

	std::vector<id_type>& pages = (*it).second->m_pages;
	size_t cNext = 0;
	size_t cTotal = pages.size();

	len = (*it).second->m_length;
	*data = new byte[len];

	byte* ptr = *data;
	size_t cLen;
	size_t cRem = len;

	do
	{
		m_dataFile.seekg(pages[cNext] * m_pageSize, std::ios_base::beg);
		if (m_dataFile.fail())
			throw Tools::IllegalStateException("SpatialIndex::DiskStorageManager: Corrupted data file.");

		m_dataFile.read(reinterpret_cast<char*>(m_buffer), m_pageSize);
		if (m_dataFile.fail())
			throw Tools::IllegalStateException("SpatialIndex::DiskStorageManager: Corrupted data file.");

		cLen = (cRem > m_pageSize) ? m_pageSize : cRem;
		memcpy(ptr, m_buffer, cLen);

		ptr += cLen;
		cRem -= cLen;
		++cNext;
	}
	while (cNext < cTotal);
}

void DiskStorageManager::storeByteArray(id_type& id, const size_t len, const byte* const data)
{
	if (id == NewPage)
	{
		Entry* e = new Entry();
		e->m_length = len;

		const byte* ptr = data;
		id_type cPage;
		size_t cRem = len;
		size_t cLen;

		while (cRem > 0)
		{
			if (! m_emptyPages.empty())
			{
				cPage = m_emptyPages.top(); m_emptyPages.pop();
			}
			else
			{
				cPage = m_nextPage;
				++m_nextPage;
			}

			cLen = (cRem > m_pageSize) ? m_pageSize : cRem;
			memcpy(m_buffer, ptr, cLen);

			m_dataFile.seekp(cPage * m_pageSize, std::ios_base::beg);
			if (m_dataFile.fail())
				throw Tools::IllegalStateException("SpatialIndex::DiskStorageManager: Corrupted data file.");
			
			m_dataFile.write(reinterpret_cast<const char*>(m_buffer), m_pageSize);
			if (m_dataFile.fail())
				throw Tools::IllegalStateException("SpatialIndex::DiskStorageManager: Corrupted data file.");

			ptr += cLen;
			cRem -= cLen;
			e->m_pages.push_back(cPage);
		}

		id = e->m_pages[0];
		m_pageIndex.insert(std::pair<id_type, Entry*>(id, e));
	}
	else
	{
		// find the entry.
		std::map<id_type, Entry*>::iterator it = m_pageIndex.find(id);

		// check if it exists.
		if (it == m_pageIndex.end())
			throw Tools::IndexOutOfBoundsException(id);

		Entry* oldEntry = (*it).second;

		m_pageIndex.erase(it);

		Entry* e = new Entry();
		e->m_length = len;

		const byte* ptr = data;
		id_type cPage;
		size_t cRem = len;
		size_t cLen, cNext = 0;

		while (cRem > 0)
		{
			if (cNext < oldEntry->m_pages.size())
			{
				cPage = oldEntry->m_pages[cNext];
				cNext++;
			}
			else if (! m_emptyPages.empty())
			{
				cPage = m_emptyPages.top(); m_emptyPages.pop();
			}
			else
			{
				cPage = m_nextPage;
				m_nextPage++;
			}

			cLen = (cRem > m_pageSize) ? m_pageSize : cRem;
			memcpy(m_buffer, ptr, cLen);

			m_dataFile.seekp(cPage * m_pageSize, std::ios_base::beg);
			if (m_dataFile.fail())
				throw Tools::IllegalStateException("SpatialIndex::DiskStorageManager: Corrupted data file.");
			
			m_dataFile.write(reinterpret_cast<const char*>(m_buffer), m_pageSize);
			if (m_dataFile.fail())
				throw Tools::IllegalStateException("SpatialIndex::DiskStorageManager: Corrupted data file.");

			ptr += cLen;
			cRem -= cLen;
			e->m_pages.push_back(cPage);
		}

		while (cNext < oldEntry->m_pages.size())
		{
			m_emptyPages.push(oldEntry->m_pages[cNext]);
			++cNext;
		}

		m_pageIndex.insert(std::pair<id_type, Entry*>(id, e));
		delete oldEntry;
	}
}

void DiskStorageManager::deleteByteArray(const id_type id)
{
	std::map<id_type, Entry*>::iterator it = m_pageIndex.find(id);

	if (it == m_pageIndex.end())
		throw Tools::InvalidPageException(id);

	for (size_t cIndex = 0; cIndex < (*it).second->m_pages.size(); ++cIndex)
	{
		m_emptyPages.push((*it).second->m_pages[cIndex]);
	}

	delete (*it).second;
	m_pageIndex.erase(it);
}
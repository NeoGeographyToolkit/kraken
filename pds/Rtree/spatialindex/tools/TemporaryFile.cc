// Tools Library
//
// Copyright (C) 2004  Navel Ltd.
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

#ifndef _MSC_VER
#include <stdio.h>
#include <unistd.h>
#else
#include <io.h>
#endif

#include "../include/tools/Tools.h"

Tools::TemporaryFile::TemporaryFile()
: m_currentFile(0),
  m_fileSize(0),
  m_bEOF(false)
{
	char p[5 + 6] = "tmpfXXXXXX";

#ifndef _MSC_VER
	int fd = mkstemp(p);
	if (fd == -1)
#else
	if (_mktemp(p) != 0)
#endif
		throw IllegalStateException(
			"Tools::TemporaryFile::TemporaryFile: Cannot create tmp file."
		);

#ifndef _MSC_VER
	close(fd);
#endif

	m_file.open(p, std::ios::in | std::ios::out | std::ios::trunc | std::ios::binary);

	if (! m_file)
		throw IllegalStateException(
			"Tools::TemporaryFile::TemporaryFile: Cannot open tmp file."
		);
	m_strFileName.push_back(std::string(p));
}

Tools::TemporaryFile::~TemporaryFile()
{
	m_file.close();

	bool bFailed = false;
	for (size_t cFile = 0; cFile < m_strFileName.size(); cFile++)
	{
		if (remove(m_strFileName[cFile].c_str()) == -1) bFailed = true;
	}

	if (bFailed)
		throw IllegalStateException(
			"Tools::TemporaryFile::~TemporaryFile: Cannot remove tmp file."
		);
}

void Tools::TemporaryFile::storeNextObject(
	size_t len,
	const byte* const data
) {
	if (m_fileSize > 1073741824L)
	{
		char p[5 + 6] = "tmpfXXXXXX";
#ifndef _MSC_VER
		int fd = mkstemp(p);
		if (fd == -1)
#else
		if (_mktemp(p) != 0)
#endif
			throw IllegalStateException(
				"Tools::TemporaryFile::storeNextObject: Cannot create tmp file."
			);

#ifndef _MSC_VER
		close(fd);
#endif

		m_file.close();
		m_file.clear();
		m_file.open(p, std::ios::in | std::ios::out | std::fstream::trunc | std::fstream::binary);
		if (! m_file)
			throw IllegalStateException(
				"Tools::TemporaryFile::storeNextObject: Cannot open tmp file."
			);

		m_strFileName.push_back(std::string(p));
		m_currentFile++;
		m_fileSize = 0;
	}

	m_file.write(reinterpret_cast<char*>(&len), sizeof(size_t));
	m_file.write(reinterpret_cast<const char*>(data), len);

	if (! m_file.good())
		throw IllegalStateException(
			"Tools::TemporaryFile::storeNextObject: Cannot store object."
		);

	m_fileSize += len + sizeof(size_t);
}

void Tools::TemporaryFile::storeNextObject(ISerializable* r)
{
	size_t len;
	byte* data;
	r->storeToByteArray(&data, len);

	try
	{
		assert(len > 0);
		storeNextObject(len, data);
		delete[] data;
	}
	catch (...)
	{
		delete[] data;
		throw;
	}
}

void Tools::TemporaryFile::loadNextObject(byte** data, size_t& len)
{
	if (m_bEOF)
		throw EndOfStreamException(
			"Tools::TemporaryFile::loadNextObject: End of file."
		);

	m_file.read(reinterpret_cast<char*>(&len), sizeof(size_t));

	if (! m_file.good())
	{
		if (m_currentFile == m_strFileName.size() - 1)
		{
			m_bEOF = true;
			throw EndOfStreamException(
				"Tools::TemporaryFile::loadNextObject: End of file."
			);
		}

		m_currentFile++;
		m_file.close();
		m_file.clear();
		m_file.open(
			m_strFileName[m_currentFile].c_str(),
			std::ios::in | std::ios::out | std::ios::binary
		);

		if (! m_file)
			throw IllegalStateException(
				"Tools::TemporaryFile::loadNextObject: Cannot open tmp file."
			);

		m_fileSize = 0;

		m_file.read(reinterpret_cast<char*>(&len), sizeof(size_t));

		if (! m_file.good())
			throw IllegalStateException(
				"Tools::TemporaryFile::loadNextObject: Cannot load length."
			);
	}

	*data = new byte[len];
	m_file.read(reinterpret_cast<char*>(*data), len);

	if (! m_file.good())
	{
		delete[] *data;
		throw IllegalStateException(
			"Tools::TemporaryFile::loadNextObject: Cannot load data."
		);
	}
}

void Tools::TemporaryFile::loadNextObject(ISerializable* r)
{
	size_t len;
	byte* data;
	loadNextObject(&data, len);
	r->loadFromByteArray(data);
	delete[] data;
}

void Tools::TemporaryFile::rewindForReading()
{
	m_file.close();
	m_file.clear();
	m_file.open(
		m_strFileName[0].c_str(),
		std::ios::in | std::ios::out | std::ios::binary
	);

	if (! m_file)
		throw IllegalStateException(
			"Tools::TemporaryFile::rewindForReading: "
			"Cannot open file" + m_strFileName[0]
		);

	m_currentFile = 0;
	m_fileSize = 0;
	m_bEOF = false;
}

void Tools::TemporaryFile::rewindForWriting()
{
	bool bFailed = false;
	for (size_t cFile = 0; cFile < m_strFileName.size(); cFile++)
	{
		if (remove(m_strFileName[cFile].c_str()) == -1) bFailed = true;
	}

	if (bFailed)
		throw IllegalStateException(
			"Tools::TemporaryFile::rewindForWriting: Cannot remove tmp file."
		);

	std::string str = m_strFileName[0];
	m_strFileName.clear();

	m_file.close();
	m_file.clear();
	m_file.open(
		str.c_str(),
		std::ios::in | std::ios::out | std::ios::trunc | std::ios::binary
	);

	if (! m_file)
		throw IllegalStateException(
			"Tools::TemporaryFile::rewindForWriting: "
			"Cannot open file " + str
		);

	m_strFileName.push_back(str);
	m_currentFile = 0;
	m_fileSize = 0;
	m_bEOF = false;
}
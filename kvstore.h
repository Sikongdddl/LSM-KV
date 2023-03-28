#pragma once

#include "kvstore_api.h"
#include "skiplist.h"
#include "bloomFliter.h"
#include "SSTable.h"
#include <fstream>
#include <vector>
#include <algorithm>

#define MAXSIZE 2*1000*1000


struct kv
{
	uint64_t key;
	std::string val;
	bool saveFlag;
	int time;
	kv()
	{
		val = "";
		saveFlag = 0;
	}
};

class MemTable
{
private:
public:
	SkipList slist;
	uint64_t currentByte;
	uint64_t numOfKV;
	MemTable()
	{
		currentByte = 10240 + 32;
		numOfKV = 0;
	}
	~MemTable()
	{
	}
	void clearit()
	{
		SKNode *n1 = this->slist.head;
		SKNode *n2;
		while (n1)
		{
			n2 = n1->forwards[0];
			delete n1;
			n1 = n2;
		}
		currentByte = 10240+32;
		numOfKV = 0;

		this->slist.head = new SKNode(0, "0", SKNodeType::HEAD);
        this->slist.NIL = new SKNode(INT_MAX, "0", SKNodeType::NIL);
        for (int i = 0; i < MAX_LEVEL; ++i)
        {
            this->slist.head->forwards[i] = this->slist.NIL;
        }
	}
};
class KVStore : public KVStoreAPI {
	// You can add your implementation here
private:
	MemTable Mem;
	void createTable(uint64_t Level, uint64_t timestrap);
	std::string dir;
public:
	KVStore(const std::string &dir);

	~KVStore();

	void put(uint64_t key, const std::string &s) override;

	std::string get(uint64_t key) override;

	bool del(uint64_t key) override;

	void reset() override;

	void scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string> > &list) override;

	int Compaction(int lower, int higher);

	int Compaction01();

	static bool comp(const kv &a, const kv &b)
	{
		return a.key < b.key;
	}

	void deleteCache(uint64_t layer, std::string path)
	{
		uint64_t idx = 0;
		for(; idx < Cache.size();++idx)
		{
			if(Cache[idx].layer == layer) break; 
		}
		//gain Cache[idx]:layer to be operate
		uint64_t length = Cache[idx].CacheSet.size();
		for(uint64_t i = 0; i < length; ++i)
		{
			if(Cache[idx].CacheSet[i].filename == path && Cache[idx].CacheSet[i].existFlag == 1)
			{
				Cache[idx].CacheSet[i].existFlag = 0;
			}
		}
		return;
	}

	std::vector<kv> merge(std::vector<kv> a, std::vector<kv> b)
	{
		std::vector<kv> res;
		uint64_t ai = 0, bi = 0;
		while(ai < a.size() && bi < b.size())
		{
			if(a[ai].key <= b[bi].key)
				res.push_back(a[ai++]);
			else
				res.push_back(b[bi++]);
		}
		if(ai == a.size())
			res.insert(res.end(), b.begin()+bi, b.end());
		else if(bi == b.size())
			res.insert(res.end(), a.begin()+ai, a.end());

		return res;
	}
	
	
};

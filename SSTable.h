#pragma once
#include <iostream>
#include <cstdint>
#include "kvstore_api.h"
#include "skiplist.h"
#include "bloomFliter.h"
#include <vector>


struct offsetBlock
{
	uint64_t key;
	uint32_t offset;
};

class SSCache
{
private:
public:
	std::string filename;
	bool existFlag;
	uint64_t* header;
	bloomFliter bf;
	std::vector<offsetBlock> offset;
	SSCache()
	{
		header = new uint64_t [4];
		filename = "";
		existFlag = 1;
	}
	~SSCache()
	{
		
	}
};
class CacheLayer
{
private:
public:
	uint64_t layer;
	std::vector<SSCache> CacheSet; 
	void traverse()
	{
		for(uint64_t i = 0; i < CacheSet.size();++i)
		{
			if(CacheSet[i].existFlag==1) std::cout<<CacheSet[i].filename<<std::endl;
		}
	}
};

extern std::vector<CacheLayer> Cache;

class SSTable
{
private:
	uint64_t timestamp;
	uint64_t numOfKV;
public:
	uint64_t offsetIndex;
	uint64_t* header;
	bloomFliter bf;
	std::vector<offsetBlock> offset;
	std::vector<std::string> data;

	void buildCache(uint64_t layer, std::string& path)
    {
        SSCache cache;
        for(int i = 0; i < 4; ++i)
        {
            cache.header[i] = this->header[i];
        }
        cache.bf = this->bf;
        cache.offset = this->offset;
		cache.filename = path;

        if(Cache.size() <= layer)
		{
			CacheLayer ltmp;
			ltmp.layer = layer;
			ltmp.CacheSet.push_back(cache);
			Cache.push_back(ltmp);
		}
		else
		{
			Cache[layer].CacheSet.push_back(cache);
		}
    }
	SSTable()
	{
		header = new uint64_t [4];
		offsetIndex = 0;
	}
	
	~SSTable()
	{
		
	}
};
#pragma once
#include "MurmurHash3.h"

/**
 * 10240 bytes bloom Fliter:
 * 
 * 
 */
class bloomFliter
{
private:
	uint32_t HashTemi[4];
public:
	bool tag[10240] = {0};
	bool search(uint64_t key)
	{
		bool flag = 1;
		MurmurHash3_x64_128(&key,sizeof(key),1,HashTemi);
		for(int i = 0; i < 4; ++i)
		{
			HashTemi[i] = HashTemi[i] % 10240;
			if(tag[HashTemi[i]] == 0) flag=0;
		}
		return flag;
	}
	void myhash(uint64_t key)
	{
		MurmurHash3_x64_128(&key,sizeof(key),1,HashTemi);
		for(int i = 0; i < 4; ++i)
		{
			HashTemi[i] = HashTemi[i] % 10240;
			tag[HashTemi[i]] = 1;
		}
	}

	bloomFliter()
	{
		for(int i = 0; i < 4; ++i)
		{
			HashTemi[i] = 0;
		}
	}

	~bloomFliter()
	{

	}
};
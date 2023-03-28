#include "kvstore.h"
#include <string>
#include <vector>

struct scancmp
{
	bool operator()(const std::pair<uint64_t, std::string>& a, const std::pair<uint64_t, std::string>& b)
	{
		return a.first < b.first;
	}
};

int ssNum = 1;
std::vector<CacheLayer> Cache;
/**
 * to do:
 * 10h
 * 
 * scan function
 * 
 * compaction further more
 * 
 */
KVStore::KVStore(const std::string &dir): KVStoreAPI(dir)
{
	this->dir = dir;
}

KVStore::~KVStore()
{
	//createTable(0,ssNum);
	//++ssNum;
}

/**
 * Create a SSTable with current MemTable
 */

void KVStore::createTable(uint64_t Level, uint64_t timestrap)
{
	SSTable ss;
	//header
	ss.header[0] = timestrap;
	ss.header[1] = Mem.numOfKV;
	uint64_t minK;
	uint64_t maxK;
	minK = Mem.slist.getminK();
	maxK = Mem.slist.getmaxK();
	ss.header[2] = minK;
	ss.header[3] = maxK;
	//bloomfliter offset && data:

	SKNode* gjr = Mem.slist.head->forwards[0];
	offsetBlock btmp;
	//traverse current main memory, alittle silly
	while((gjr)->type != NIL)
	{
		ss.bf.myhash(gjr->key);
		btmp.key = gjr->key;
		btmp.offset = ss.offsetIndex;
		ss.offsetIndex += gjr->val.length();
		ss.offset.push_back(btmp);

		ss.data.push_back(gjr->val);
		gjr = gjr->forwards[0];
	}

	std::ofstream testfile;
	std::vector<std::string> pathSet; 

	int levelnum = utils::scanDir(this->dir,pathSet);

	if((uint64_t)levelnum <= Level)
	{
		utils::mkdir((this->dir + "/Level"+std::to_string(Level)).c_str());
	}

	std::string name = std::to_string(ssNum);
	++ssNum;
	std::string level =std::to_string(Level);

	//cache
	std::string curpath = this->dir + "/Level"+level+"/"+name+".sst";
	ss.buildCache(Level,curpath);

	//file_push
	testfile.open(curpath, std::ios::binary);
	for(int i = 0; i < 4; ++i)
	{
		testfile.write((char*)& ss.header[i], sizeof(uint64_t));
	}
	for(int i = 0; i < 10240; ++i)
	{
		testfile.write((char*)& ss.bf.tag[i], sizeof(bool));
	}
	for(uint64_t i = 0; i < ss.header[1]; ++i)
	{
		testfile.write((char*)& ss.offset[i].key, sizeof(ss.offset[i].key));
		testfile.write((char*)& ss.offset[i].offset, sizeof(ss.offset[i].offset));
	}
	for(uint64_t i = 0; i < ss.header[1]; ++i)
	{
		testfile.write((ss.data[i].c_str()), ss.data[i].size());
	}
	testfile.close();
	Mem.clearit();

	int cFlag = 0;
	if(Level == 0) cFlag = Compaction01();
	if(cFlag == 0) return;
	else if(cFlag == 1)
	{
		int comIndex = 1;
		while(cFlag != 0)
		{
			cFlag = Compaction(comIndex, comIndex+1);
			++comIndex;
		}
		return;
	}
	else return;
}
/**
 * Insert/Update the key-value pair.
 * No return values for simplicity.
 */
void KVStore::put(uint64_t key, const std::string &s)
{
	//std::cout<<"insert!"<<key<<std::endl;
	//size test
	int length = s.length();

	std::string searchrst = this->Mem.slist.Search(key);
	if(searchrst == "")
	{
		if((this->Mem.currentByte + 12 + length) > MAXSIZE)
		{
			this->createTable(0,ssNum);
		}
		this->Mem.slist.Insert(key,s);
		++(this->Mem.numOfKV);
		this->Mem.currentByte += (12+length);
	}
	else
	{
		int curlength = s.length();
		int pastlength = searchrst.length();
		if(curlength <= pastlength)
		{
			this->Mem.slist.Insert(key,s);
			this->Mem.currentByte+=curlength;
			this->Mem.currentByte-=pastlength;
		}
		else
		{
			bool createFlag = 0;
			if((this->Mem.currentByte + curlength-pastlength) > MAXSIZE)
			{
				createFlag = 1;
				this->createTable(0,ssNum);
			}
			this->Mem.slist.Insert(key,s);
			if(createFlag)
			{
				++(this->Mem.numOfKV);
				this->Mem.currentByte += (12 + curlength);
			}
			else
			{
				this->Mem.currentByte += (curlength - pastlength);
			}
			
		}
	}
	//std::cout<<this->Mem.currentByte<<std::endl;
}
/**
 * Returns the (string) value of the given key.
 * An empty string indicates not found.
 */
std::string KVStore::get(uint64_t key)
{
	//std::cout<<"get!"<<key<<" ";
	int valtime = 0;
	struct findresult
	{
		std::string result;
		uint64_t timestrap;
		findresult()
		{
			result = "";
			timestrap = 0;
		}
	};
	findresult result;
	std::string searchVal = this->Mem.slist.Search(key);
	if(searchVal!="")
	{
		if(searchVal != "~DELETED~")
		{
			//std::cout<<searchVal.length()<<std::endl;
			return searchVal;
		}
		else
		{
			//std::cout<<"choose deleted in memory"<<std::endl;
			return "";
		}
		//std::cout<<"find "<<key<<" in memtable!"<<std::endl;
	}
	else//visit SStable
	{
		int layerNum = Cache.size();
		for(int i = 0; i <layerNum; ++i)
		{
			int cacheSize = Cache[i].CacheSet.size();
			for(int j = 0; j < cacheSize; ++j)
			{
				//std::cout<<"in: layer"<<i<<", block"<<j;
				while(j < cacheSize && Cache[i].CacheSet[j].existFlag==0)
				{
					++j;
				}
				if(j != cacheSize && Cache[i].CacheSet[j].bf.search(key)==1)
				{
					uint32_t thisOff = 0;
					uint64_t left = 0;
					uint64_t cursize = Cache[i].CacheSet[j].offset.size();
					while(left != cursize && Cache[i].CacheSet[j].offset[left].key != key)
					{
						++left;
					}
					if(left != cursize)
					{
						++valtime;
						uint64_t right = left+1;
						thisOff = Cache[i].CacheSet[j].offset[left].offset;
						std::ifstream readfile;
						readfile.open(Cache[i].CacheSet[j].filename, std::ios::binary);
						SSTable sstmp;
						for(int p = 0; p < 4; ++p)
						{
							readfile.read((char*)& sstmp.header[p], sizeof(uint64_t));
						}
						for(int p = 0; p < 10240; ++p)
						{
							readfile.read((char*)& sstmp.bf.tag[p], sizeof(bool));
						}
						for(uint64_t p = 0; p < sstmp.header[1]; ++p)
						{
							offsetBlock tmp;
							readfile.read((char*)& tmp.key, sizeof(sstmp.offset[p].key));
							readfile.read((char*)& tmp.offset, sizeof(sstmp.offset[p].offset));
							sstmp.offset.push_back(tmp);
							//std::cout<<i<<" "<<tmp.offset<<std::endl;
						}
						std::string tmp = "";

						//get value from thisOff
						readfile.seekg(thisOff, std::ifstream::cur);
						/*for(uint64_t p = 0; p < thisOff; ++p)
						{
							char tt = readfile.get();
						}*/

						if(left+1 != sstmp.header[1])
						{	
							uint64_t nextOff = Cache[i].CacheSet[j].offset[right].offset;
							uint64_t readlen = nextOff - thisOff;
							for(uint64_t p = 0; p < readlen; ++p)
							{
								char tt = readfile.get();
								tmp += tt;
							}
						}
						else
						{
							char tt = readfile.get();
							while(!readfile.eof())
							{
								tmp += tt;
								tt = readfile.get();
							}
						}
						readfile.close();
						//std::cout<<tmp.length()<<std::endl;
						//std::cout<<"have find!"<<key<<std::endl;
						if(sstmp.header[0] > result.timestrap)
						{
							result.timestrap = sstmp.header[0];
							result.result = tmp;
						}
					}
				}
			}
		}
	}
	if(result.result == "~DELETED~")
	{
		//std::cout<<valtime<<"times"<<std::endl;
		return "";
	}
	else 
	{
		//std::cout<<valtime<<"times"<<std::endl;
		return result.result;
	}
}
/**
 * Delete the given key-value pair if it exists.
 * Returns false iff the key is not found.
 */
bool KVStore::del(uint64_t key)
{
	std::string searchVal = this->get(key);
	if(searchVal == "")//not found
	{
		return false;
	}
	else if(searchVal == "~DELETED~")//not found
	{
		return false;
	}
	else//found
	{
		std::string deltmp = "~DELETED~";
		this->put(key, deltmp);
		return true;
	}
}

/**
 * This resets the kvstore. All key-value pairs should be removed,
 * including memtable and all sstables files.
 */
void KVStore::reset()
{
	std::vector<std::vector<std::string>> filepath;
	std::vector<std::string> dirpath;
	int filenum = 0;
	int levelnum = utils::scanDir(this->dir,dirpath);
	//std::cout<<"levelnum:"<<levelnum<<std::endl;
	for(int i = 0; i < levelnum; ++i)
	{
		std::string stri = std::to_string(i);
		std::vector<std::string> tmp;
		filenum += utils::scanDir(this->dir+"/Level"+stri, tmp);
		filepath.push_back(tmp);
	}
	for(uint64_t i = 0; i < dirpath.size(); ++i)
	{
		std::string stri = std::to_string(i);
		for(uint64_t k = 0; k < filepath[i].size(); ++k)
		{
			utils::rmfile((this->dir +"/Level"+stri+"/"+filepath[i][k]).c_str());
		}
	}
	for(uint64_t i = 0; i < dirpath.size(); ++i)
	{
		utils::rmdir((this->dir + "/"+ dirpath[i]).c_str());
	}
	Cache.clear();
	Mem.clearit();
	return;
}

/**
 * Return a list including all the key-value pair between key1 and key2.
 * keys in the list should be in an ascending order.
 * An empty string indicates not found.
 */
void KVStore::scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string> > &list)
{
	std::vector<std::list<std::pair<uint64_t, std::string>>> results;
	std::list<std::pair<uint64_t, std::string>> mem_found;
	std::list<std::pair<uint64_t, std::string>> ss_found;

	std::string tmp;
	std::pair<uint64_t, std::string> pairtmp;

	//in memtable:
	SKNode* p =this->Mem.slist.head;
	while(p->forwards[0]->type != NIL)
	{
		if(p->forwards[0]->key >= key1 && p->forwards[0]->key <= key2)
		{
			pairtmp.first = p->forwards[0]->key;
			pairtmp.second = p->forwards[0]->val;
			mem_found.push_back(pairtmp);
		}
		p = p->forwards[0];
	}
	results.push_back(mem_found);

	if(!utils::dirExists(this->dir + "/Level0"))
	{
		list = mem_found;
		return;
	}
	else
	{
		//in sstable:
		//in Level0: visit every key
		int filenum0 = 0;
		std::vector<std::string> pathVec;
		filenum0 = utils::scanDir(this->dir + "/Level0",pathVec);
		for(int k = 0; k < filenum0; ++k)
		{
			std::ifstream readfile;
			readfile.open(this->dir + "/Level0/"+pathVec[k],std::ios::binary);
			SSTable sstmp;
			for(int i = 0; i < 4; ++i)
			{
				readfile.read((char*)& sstmp.header[i], sizeof(uint64_t));
			}
			if(sstmp.header[2]> key2 || sstmp.header[3] < key1)
			{
				readfile.close();
				continue;
			}
			for(int i = 0; i < 10240; ++i)
			{
				readfile.read((char*)& sstmp.bf.tag[i], sizeof(bool));
			}
			for(uint64_t i = 0; i < sstmp.header[1]; ++i)
			{
				offsetBlock tmp;
				readfile.read((char*)& tmp.key, sizeof(sstmp.offset[i].key));
				readfile.read((char*)& tmp.offset, sizeof(sstmp.offset[i].offset));
				sstmp.offset.push_back(tmp);
				//std::cout<<i<<" "<<tmp.offset<<std::endl;
			}
			for(uint64_t i = 0; i < sstmp.header[1]; ++i)
			{
				std::string tmp = "";
				if(i+1!=sstmp.header[1])
				{
					uint64_t readlen = sstmp.offset[i+1].offset - sstmp.offset[i].offset;
					//std::cout<<"readlen:"<<readlen << i <<" ";
					for(uint64_t p = 0; p < readlen; ++p)
					{
						char tt = readfile.get();
						tmp += tt;
					}
					//std::cout<<"val = :"<<tmp<<std::endl;
					sstmp.data.push_back(tmp);
				}
				else
				{
					char tt = readfile.get();
					while(!readfile.eof())
					{
						tmp += tt;
						tt = readfile.get();
					}
					sstmp.data.push_back(tmp);
				}
			}
			readfile.close();
			//finished load all data
			for(uint64_t i = 0; i < sstmp.header[1]; ++i)
			{
				if(sstmp.offset[i].key >= key1 && sstmp.offset[i].key <= key2)
				{
					pairtmp.first = sstmp.offset[i].key;
					pairtmp.second = sstmp.data[i];
					ss_found.emplace_back(pairtmp);
				}
			}
		}
		results.push_back(ss_found);
		ss_found.clear();
		//in deeper levels:check Cache to decrease calculations
		int layerNum = Cache.size();
		for(int layerindex = 1;layerindex < layerNum; ++layerindex)
		{
			ss_found.clear();
			int cacheSize = Cache[layerindex].CacheSet.size();
			for(int j = 0; j < cacheSize; ++j)
			{
				while(j < cacheSize && Cache[layerindex].CacheSet[j].existFlag==0)
				{
					++j;
				}
				if(Cache[layerindex].CacheSet[j].header[2]>key2 || Cache[layerindex].CacheSet[j].header[3] < key1)
				{
					continue;
				}
				else if(j != cacheSize)
				{
					int filenumCur = 0;
					std::ifstream readfile;
					readfile.open(Cache[layerindex].CacheSet[j].filename,std::ios::binary);
					SSTable sstmp;
					for(int i = 0; i < 4; ++i)
					{
						readfile.read((char*)& sstmp.header[i], sizeof(uint64_t));
					}
					for(int i = 0; i < 10240; ++i)
					{
						readfile.read((char*)& sstmp.bf.tag[i], sizeof(bool));
					}
					for(uint64_t i = 0; i < sstmp.header[1]; ++i)
					{
						offsetBlock tmp;
						readfile.read((char*)& tmp.key, sizeof(sstmp.offset[i].key));
						readfile.read((char*)& tmp.offset, sizeof(sstmp.offset[i].offset));
						sstmp.offset.push_back(tmp);
						//std::cout<<i<<" "<<tmp.offset<<std::endl;
					}
					for(uint64_t i = 0; i < sstmp.header[1]; ++i)
					{
						std::string tmp = "";
						if(i+1!=sstmp.header[1])
						{
							uint64_t readlen = sstmp.offset[i+1].offset - sstmp.offset[i].offset;
							//std::cout<<"readlen:"<<readlen << i <<" ";
							for(uint64_t p = 0; p < readlen; ++p)
							{
								char tt = readfile.get();
								tmp += tt;
							}
							//std::cout<<"val = :"<<tmp<<std::endl;
							sstmp.data.push_back(tmp);
						}
						else
						{
							char tt = readfile.get();
							while(!readfile.eof())
							{
								tmp += tt;
								tt = readfile.get();
							}
							sstmp.data.push_back(tmp);
						}
					}
					readfile.close();
					//finished load all data
					for(uint64_t i = 0; i < sstmp.header[1]; ++i)
					{
						if(sstmp.offset[i].key >= key1 && sstmp.offset[i].key <= key2)
						{
							pairtmp.first = sstmp.offset[i].key;
							pairtmp.second = sstmp.data[i];
							ss_found.emplace_back(pairtmp);
						}
					}
				}
			}
			results.push_back(ss_found);
		}
	}

	uint64_t numOfList = results.size();

	bool continue_flag = 1;
	while(continue_flag)
	{
		continue_flag = 0;
		bool found = 0;
		uint64_t curIndex;
		uint64_t curMinKey;
		for(uint64_t i = 0; i < numOfList; ++i)
		{
			if(!results[i].empty())
			{
				continue_flag = 1;
				pairtmp = results[i].front();
				if(!found)
				{
					found = 1;
					curIndex = i;
					curMinKey = pairtmp.first;
				}
				else
				{
					if(pairtmp.first < curMinKey)
					{
						curIndex = i;
						curMinKey = pairtmp.first;
					}
					else if(pairtmp.first == curMinKey)
					{
						results[i].pop_front();
					}
				}
			}
		}
		if(found)
		{
			pairtmp = results[curIndex].front();
			results[curIndex].pop_front();
			if(pairtmp.second != "~DELETED")
			{
				list.emplace_back(pairtmp);
			}
		}
	}
	//list.sort(scancmp());
	/*for(uint64_t m = 0; m < list.size(); ++m)
	{
		std::cout<<list.front().first<<std::endl;
		list.pop_front();
	}*/
	return;
}
/**
 * return 0 if not compaction, 1 if compaction
 */
int KVStore::Compaction01()
{
	std::vector<std::string> pathVec;
	std::vector<std::string> path1Vec;
	std::vector<std::string> rm0Vec;
	std::vector<std::string> rm1Vec;
	std::vector<kv> kvs;
	kv gjr;
	int filenum0 = 0;
	int filenum1 = 0;
	filenum0 = utils::scanDir(this->dir+"/Level0",pathVec);
	if(filenum0 <= 2)
	{
		pathVec.clear();
		return 0;
	}
	else//level0 all files compaction to level1:
	{
		uint64_t mink = 2147483647;
		uint64_t maxk = 0;
		uint64_t maxTime = 0;
		//load level0
		for(int k = 0; k < filenum0; ++k)
		{
			std::ifstream readfile;
			readfile.open(this->dir + "/Level0/"+pathVec[k],std::ios::binary);
			rm0Vec.push_back(this->dir + "/Level0/"+pathVec[k]);
			SSTable sstmp;
			for(int i = 0; i < 4; ++i)
			{
				readfile.read((char*)& sstmp.header[i], sizeof(uint64_t));
			}
			for(int i = 0; i < 10240; ++i)
			{
				readfile.read((char*)& sstmp.bf.tag[i], sizeof(bool));
			}
			for(uint64_t i = 0; i < sstmp.header[1]; ++i)
			{
				offsetBlock tmp;
				readfile.read((char*)& tmp.key, sizeof(sstmp.offset[i].key));
				readfile.read((char*)& tmp.offset, sizeof(sstmp.offset[i].offset));
				sstmp.offset.push_back(tmp);
				//std::cout<<i<<" "<<tmp.offset<<std::endl;
			}
			for(uint64_t i = 0; i < sstmp.header[1]; ++i)
			{
				std::string tmp = "";
				if(i+1!=sstmp.header[1])
				{
					uint64_t readlen = sstmp.offset[i+1].offset - sstmp.offset[i].offset;
					//std::cout<<"readlen:"<<readlen << i <<" ";
					for(uint64_t p = 0; p < readlen; ++p)
					{
						char tt = readfile.get();
						tmp += tt;
					}
					//std::cout<<"val = :"<<tmp<<std::endl;
					sstmp.data.push_back(tmp);
				}
				else
				{
					char tt = readfile.get();
					while(!readfile.eof())
					{
						tmp += tt;
						tt = readfile.get();
					}
					sstmp.data.push_back(tmp);
				}
			}
			readfile.close();
			//finished load all data

			for(uint64_t i = 0; i < sstmp.header[1]; ++i)
			{
				gjr.key = sstmp.offset[i].key;
				gjr.val = sstmp.data[i];
				gjr.time = sstmp.header[0];
				gjr.saveFlag = 1;
				kvs.push_back(gjr);
			}
			if(sstmp.header[2]<mink) mink = sstmp.header[2];
			if(sstmp.header[3]>maxk) maxk = sstmp.header[3];
			if(sstmp.header[0] > maxTime) maxTime = sstmp.header[0];
		}
		//already load all files in level0
		//optionally load level1

		if(!utils::dirExists(this->dir + "/Level1"))
		{
			std::string dirtmp = this->dir + "/Level1";
			utils::mkdir(dirtmp.c_str());
		}
		else // optionally load level 1
		{
			filenum1 = utils::scanDir(this->dir+"/Level1", path1Vec);
			//load level1
			for(int k = 0; k < filenum1; ++k)
			{
				//choose key range
				int cacheIndex = 0;
				while(Cache[1].CacheSet[cacheIndex].existFlag==0 || Cache[1].CacheSet[cacheIndex].filename != (this->dir + "/Level1/"+path1Vec[k]))
				{
					cacheIndex++;
				}
				if(Cache[1].CacheSet[cacheIndex].header[2] > maxk || Cache[1].CacheSet[cacheIndex].header[3] < mink)
				{
					continue;
				}
				//should be compactied:
				std::ifstream readfile;
				readfile.open(this->dir + "/Level1/"+path1Vec[k],std::ios::binary);
				rm1Vec.push_back(this->dir + "/Level1/"+path1Vec[k]);
				SSTable sstmp;
				for(int i = 0; i < 4; ++i)
				{
					readfile.read((char*)& sstmp.header[i], sizeof(uint64_t));
				}
				for(int i = 0; i < 10240; ++i)
				{
					readfile.read((char*)& sstmp.bf.tag[i], sizeof(bool));
				}
				
				for(uint64_t i = 0; i < sstmp.header[1]; ++i)
				{
					offsetBlock tmp;
					readfile.read((char*)& tmp.key, sizeof(sstmp.offset[i].key));
					readfile.read((char*)& tmp.offset, sizeof(sstmp.offset[i].offset));
					sstmp.offset.push_back(tmp);
					//std::cout<<i<<" "<<tmp.offset<<std::endl;
				}
				for(uint64_t i = 0; i < sstmp.header[1]; ++i)
				{
					std::string tmp = "";
					if(i+1!=sstmp.header[1])
					{
						uint64_t readlen = sstmp.offset[i+1].offset - sstmp.offset[i].offset;
						//std::cout<<"readlen:"<<readlen << i <<" ";
						for(uint64_t p = 0; p < readlen; ++p)
						{
							char tt = readfile.get();
							tmp += tt;
						}
						//std::cout<<"val = :"<<tmp<<std::endl;
						sstmp.data.push_back(tmp);
					}
					else
					{
						char tt = readfile.get();
						while(!readfile.eof())
						{
							tmp += tt;
							tt = readfile.get();
						}
						sstmp.data.push_back(tmp);
					}
				}
				readfile.close();
				//finished load all data

				for(uint64_t i = 0; i < sstmp.header[1]; ++i)
				{
					gjr.key = sstmp.offset[i].key;
					gjr.val = sstmp.data[i];
					gjr.time = sstmp.header[0];
					gjr.saveFlag = 1;
					kvs.push_back(gjr);
				}

				if(sstmp.header[0] > maxTime) maxTime = sstmp.header[0];
			}
		}
		uint64_t L = kvs.size();
		stable_sort(kvs.begin(),kvs.end(),comp);

		for(uint64_t i = 0; i < L; ++i)
		{
			if(kvs[i].saveFlag==0) continue;
			uint64_t indextmp = i+1;	
			//in test 2,  there isn't repeated key, so the saveFlag test is useless
			while(indextmp != L && kvs[i].key == kvs[indextmp].key)
			{	
				if(kvs[i].time < kvs[indextmp].time)
					kvs[i].saveFlag = 0;
				if(kvs[i].time > kvs[indextmp].time)
					kvs[indextmp].saveFlag = 0;
				++indextmp;
			}
		}

		for(uint64_t i = 0; i < L; ++i)
		{
			if(kvs[i].saveFlag==0) continue;
			//in test 2,  there isn't repeated key, so the saveFlag test is useless
			uint64_t length = kvs[i].val.length();
			std::string searchrst = this->Mem.slist.Search(kvs[i].key);
			if(searchrst == "")
			{
				if((this->Mem.currentByte + 12 + length) > MAXSIZE)
				{
					this->createTable(1,maxTime);
				}
				this->Mem.slist.Insert(kvs[i].key,kvs[i].val);
				++(this->Mem.numOfKV);
				this->Mem.currentByte += (12+length);
			}
			else
			{
				int curlength = kvs[i].val.length();
				int pastlength = searchrst.length();
				if(curlength <= pastlength)
				{
					this->Mem.slist.Insert(kvs[i].key,kvs[i].val);
					this->Mem.currentByte+=curlength;
					this->Mem.currentByte-=pastlength;
				}
				else
				{
					bool createFlag = 0;
					if((this->Mem.currentByte + curlength-pastlength) > MAXSIZE)
					{
						createFlag = 1;
						this->createTable(1,maxTime);
					}
					this->Mem.slist.Insert(kvs[i].key,kvs[i].val);
					if(createFlag)
					{
						++(this->Mem.numOfKV);
						this->Mem.currentByte += (12 + curlength);
					}
					else
					{
						this->Mem.currentByte += (curlength - pastlength);
					}
					
				}
			}
			/*if(this->Mem.currentByte + 12 + length > MAXSIZE)
			{
				this->createTable(1,maxTime);
			}

			bool insFlag = this->Mem.slist.Insert(kvs[i].key, kvs[i].val);
			if(insFlag ==1) 
			{
				++(this->Mem.numOfKV);
				this->Mem.currentByte += (12 + length);
				//std::cout<<"in level:"<<1<<"currentByte:"<<this->Mem.currentByte<<std::endl;
			}*/
		}
		this->createTable(1,maxTime);

		int rm0size = rm0Vec.size();
		int rm1size = rm1Vec.size();
		for(int j = 0; j < rm0size; ++j)
		{
			utils::rmfile(rm0Vec[j].c_str());
			this->deleteCache(0,rm0Vec[j]);
		}
		for(int j = 0; j < rm1size; ++j)
		{
			utils::rmfile(rm1Vec[j].c_str());
			this->deleteCache(1, rm1Vec[j]);
		}
	}
	return 1;
}
/**
 *  return: 0:not compaction
 *  return: 1:compaction to a new layer!
 */
int KVStore::Compaction(int lower, int higher)
{
	bool bottomFlag = 0;
	if(!utils::dirExists(this->dir + "/Level"+std::to_string(higher)))
	{
		bottomFlag = 1;
	}
	std::vector<std::string> pathlVec;
	std::vector<std::string> pathhVec;
	std::vector<std::string> rm0Vec;
	std::vector<std::string> rm1Vec;
	std::vector<kv> kvs;
	kv gjr;
	int capacity = 1;
	for(int i = 0; i < lower+1; ++i)
	{
		capacity *= 2;
	}
	int filenuml = utils::scanDir(this->dir + "/Level"+std::to_string(lower),pathlVec);
	int filenumh = 0;
	if(filenuml <= capacity)
	{
		return 0;
	}
	else
	{
		//std::cout<<"current:"<<lower<<"to "<<higher<<std::endl;
		//std::cout<<"capacity:"<<capacity<<std::endl;
		//std::cout<<"filenumLower:"<<filenuml<<std::endl;
		//prepare
		uint64_t mink = 2147483647;
		uint64_t maxk = 0;
		uint64_t maxTime = 0;
		//choose unnecessary files
		uint64_t killnum = filenuml - capacity;
		while(rm0Vec.size() < killnum)
		{
			uint64_t timestrapMin = 2147483647;
			int cacheIndex = 0;
			struct killBlock
			{
				uint64_t minOfK;
				uint64_t index;
			};
			std::vector<killBlock> killVec;
			//gain timestrapMin
			for(uint64_t i = 0; i < Cache[lower].CacheSet.size(); ++i)
			{
				if(Cache[lower].CacheSet[cacheIndex].existFlag == 1)
				{
					if(Cache[lower].CacheSet[cacheIndex].header[0] < timestrapMin)
					{
						timestrapMin = Cache[lower].CacheSet[cacheIndex].header[0];
					}
				}
				++cacheIndex;
			}
			cacheIndex = 0;
			//choose files to kill
			for(uint64_t i = 0; i < Cache[lower].CacheSet.size(); ++i)
			{
				if(rm0Vec.size()==killnum) break;
				if(Cache[lower].CacheSet[cacheIndex].header[0] == timestrapMin)
				{
					if(Cache[lower].CacheSet[cacheIndex].existFlag == 1)
					{
						killBlock tmp;
						tmp.index = cacheIndex;
						tmp.minOfK = Cache[lower].CacheSet[cacheIndex].header[2];
						killVec.push_back(tmp);
						//rm0Vec.push_back(Cache[lower].CacheSet[cacheIndex].filename);

						//std::cout<<"choose "<<Cache[lower].CacheSet[cacheIndex].header[0]<<std::endl;
						//Cache[lower].CacheSet[cacheIndex].existFlag = 0;
					}
				}
				++cacheIndex;
			}
			if(killVec.size() == 1)
			{
				rm0Vec.push_back(Cache[lower].CacheSet[killVec[0].index].filename);
			}
			//multi file timestrap are the same:
			//choose ones whose key is smaller
			else
			{
				if(killVec.size() + rm0Vec.size() < killnum)
				{
					int length = killVec.size();
					for(int p = 0; p < length; ++p)
					{
						rm0Vec.push_back(Cache[lower].CacheSet[killVec[p].index].filename);
					}
				}
				else
				{
					int miniKey = 2147483647;
					int miniIndex = 0;
					int length = killVec.size();
					int indexp = 0;
					for(int p = 0; p < length; ++p)
					{
						if(killVec[indexp].minOfK < miniKey)
						{
							miniKey = killVec[indexp].minOfK;
							miniIndex = indexp;
						}
						++indexp;
					}
					indexp = 0;
					rm0Vec.push_back(Cache[lower].CacheSet[killVec[miniIndex].index].filename);
				}
			}
		}
		//std::cout<<"killnum="<<killnum<<std::endl;
		for(uint64_t k = 0; k < killnum; ++k)
		{
			std::ifstream readfile; 
			readfile.open(rm0Vec[k],std::ios::binary);
			SSTable sstmp;
			for(int i = 0; i < 4; ++i)
			{
				readfile.read((char*)& sstmp.header[i], sizeof(uint64_t));
			}
			for(int i = 0; i < 10240; ++i)
			{
				readfile.read((char*)& sstmp.bf.tag[i], sizeof(bool));
			}
			
			for(uint64_t i = 0; i < sstmp.header[1]; ++i)
			{
				offsetBlock tmp;
				readfile.read((char*)& tmp.key, sizeof(sstmp.offset[i].key));
				readfile.read((char*)& tmp.offset, sizeof(sstmp.offset[i].offset));
				sstmp.offset.push_back(tmp);
				//std::cout<<i<<" "<<tmp.offset<<std::endl;
			}
			for(uint64_t i = 0; i < sstmp.header[1]; ++i)
			{
				std::string tmp = "";
				if(i+1!=sstmp.header[1])
				{
					uint64_t readlen = sstmp.offset[i+1].offset - sstmp.offset[i].offset;
					//std::cout<<"readlen:"<<readlen << i <<" ";
					for(uint64_t p = 0; p < readlen; ++p)
					{
						char tt = readfile.get();
						tmp += tt;
					}
					//std::cout<<"val = :"<<tmp<<std::endl;
					sstmp.data.push_back(tmp);
				}
				else
				{
					char tt = readfile.get();
					while(!readfile.eof())
					{
						tmp += tt;
						tt = readfile.get();
					}
					sstmp.data.push_back(tmp);
				}
			}
			readfile.close();
			//finished load all data

			for(uint64_t i = 0; i < sstmp.header[1]; ++i)
			{
				gjr.key = sstmp.offset[i].key;
				gjr.val = sstmp.data[i];
				gjr.time = sstmp.header[0];
				gjr.saveFlag = 1;
				kvs.push_back(gjr);
			}
			if(sstmp.header[2]<mink) mink = sstmp.header[2];
			if(sstmp.header[3]>maxk) maxk = sstmp.header[3];
			if(sstmp.header[0] > maxTime) maxTime = sstmp.header[0];
		}
		//now we have collected all information in low level, and gain minK, maxK.
		//next: deal with the high level:
		//optionally load level higher
		if(!utils::dirExists(this->dir + "/Level"+std::to_string(higher)))
		{
			std::string dirtmp = this->dir + "/Level"+std::to_string(higher);
			utils::mkdir(dirtmp.c_str());
		}
		else
		{
			filenumh = utils::scanDir(this->dir+"/Level"+std::to_string(higher),pathhVec);
			//std::cout<<"filenumnow:"<<filenumh<<std::endl;
			//load level1
			for(int k = 0; k < filenumh; ++k)
			{
				//choose key range
				int cacheIndex = 0;
				while(Cache[higher].CacheSet[cacheIndex].existFlag == 0 || Cache[higher].CacheSet[cacheIndex].filename != (this->dir + "/Level"+std::to_string(higher)+"/"+pathhVec[k]))
				{
					cacheIndex++;
				}
				if(Cache[higher].CacheSet[cacheIndex].header[2] > maxk || Cache[higher].CacheSet[cacheIndex].header[3] < mink)
				{
					continue;
				}
				//should be compactied:
				std::ifstream readfile; 
				readfile.open(this->dir + "/Level"+std::to_string(higher)+"/"+pathhVec[k],std::ios::binary);
				rm1Vec.push_back(this->dir + "/Level"+std::to_string(higher)+"/"+pathhVec[k]);
				SSTable sstmp;
				for(int i = 0; i < 4; ++i)
				{
					readfile.read((char*)& sstmp.header[i], sizeof(uint64_t));
				}
				for(int i = 0; i < 10240; ++i)
				{
					readfile.read((char*)& sstmp.bf.tag[i], sizeof(bool));
				}
				
				for(uint64_t i = 0; i < sstmp.header[1]; ++i)
				{
					offsetBlock tmp;
					readfile.read((char*)& tmp.key, sizeof(sstmp.offset[i].key));
					readfile.read((char*)& tmp.offset, sizeof(sstmp.offset[i].offset));
					sstmp.offset.push_back(tmp);
					//std::cout<<i<<" "<<tmp.offset<<std::endl;
				}
				for(uint64_t i = 0; i < sstmp.header[1]; ++i)
				{
					std::string tmp = "";
					if(i+1!=sstmp.header[1])
					{
						uint64_t readlen = sstmp.offset[i+1].offset - sstmp.offset[i].offset;
						//std::cout<<"readlen:"<<readlen << i <<" ";
						for(uint64_t p = 0; p < readlen; ++p)
						{
							char tt = readfile.get();
							tmp += tt;
						}
						//std::cout<<"val = :"<<tmp<<std::endl;
						sstmp.data.push_back(tmp);
					}
					else
					{
						char tt = readfile.get();
						while(!readfile.eof())
						{
							tmp += tt;
							tt = readfile.get();
						}
						sstmp.data.push_back(tmp);
					}
				}
				readfile.close();
				//finished load all data

				for(uint64_t i = 0; i < sstmp.header[1]; ++i)
				{
					gjr.key = sstmp.offset[i].key;
					gjr.val = sstmp.data[i];
					gjr.time = sstmp.header[0];
					gjr.saveFlag = 1;
					/*if(bottomFlag==1)
					{
						if(gjr.val == "~DELETED~")
						{
							gjr.saveFlag = 0;
							continue;
						}
					}
					else kvs.push_back(gjr);*/
					kvs.push_back(gjr);
				}
				if(sstmp.header[0] > maxTime) maxTime = sstmp.header[0];
			}
		}
		//std::cout<<"finished load all files in level1!"<<std::endl;
		uint64_t L = kvs.size();
		stable_sort(kvs.begin(),kvs.end(),comp);
		

		for(uint64_t i = 0; i < L; ++i)
		{
			uint64_t indextmp = i+1;
			if(kvs[i].saveFlag==0) continue;
			//in test 2,  there isn't repeated key, so the saveFlag test is useless
			while(indextmp!=L && kvs[i].key == kvs[indextmp].key)
			{	
				if(kvs[i].time < kvs[indextmp].time)
					kvs[i].saveFlag = 0;
				if(kvs[i].time > kvs[indextmp].time)
					kvs[indextmp].saveFlag = 0;
				++indextmp;
			}
		}

		for(uint64_t i = 0; i < L; ++i)
		{
			if(kvs[i].saveFlag==0) continue;
			uint64_t length = kvs[i].val.length();
			std::string searchrst = this->Mem.slist.Search(kvs[i].key);
			if(searchrst == "")
			{
				if((this->Mem.currentByte + 12 + length) > MAXSIZE)
				{
					this->createTable(higher,maxTime);
				}
				this->Mem.slist.Insert(kvs[i].key,kvs[i].val);
				++(this->Mem.numOfKV);
				this->Mem.currentByte += (12+length);
			}
			else
			{
				int curlength = kvs[i].val.length();
				int pastlength = searchrst.length();
				if(curlength <= pastlength)
				{
					this->Mem.slist.Insert(kvs[i].key,kvs[i].val);
					this->Mem.currentByte+=curlength;
					this->Mem.currentByte-=pastlength;
				}
				else
				{
					bool createFlag = 0;
					if((this->Mem.currentByte + curlength-pastlength) > MAXSIZE)
					{
						createFlag = 1;
						this->createTable(higher,maxTime);
					}
					this->Mem.slist.Insert(kvs[i].key,kvs[i].val);
					if(createFlag)
					{
						++(this->Mem.numOfKV);
						this->Mem.currentByte += (12 + curlength);
					}
					else
					{
						this->Mem.currentByte += (curlength - pastlength);
					}
					
				}
			}
			/*if(kvs[i].saveFlag==0) continue;
			uint64_t length = kvs[i].val.length();

			if(this->Mem.currentByte + 12 + length > MAXSIZE)
			{
				this->createTable(higher,maxTime);
				//std::cout<<"createTable!"<<std::endl;
			}

			bool insFlag = this->Mem.slist.Insert(kvs[i].key, kvs[i].val);
			if(insFlag ==1)
			{
				this->Mem.currentByte += (12 + length);
				++(this->Mem.numOfKV);
				//std::cout<<"in level:"<<higher<<"currentByte:"<<this->Mem.currentByte<<std::endl;
			}*/
		}
		this->createTable(higher, maxTime);
		//std::cout<<"finished create all Table!"<<std::endl;
		int rm0size = rm0Vec.size();
		int rm1size = rm1Vec.size();
		for(int j = 0; j < rm0size; ++j)
		{
			utils::rmfile(rm0Vec[j].c_str());
			this->deleteCache(lower,rm0Vec[j]);
		}
		for(int j = 0; j < rm1size; ++j)
		{
			utils::rmfile(rm1Vec[j].c_str());
			this->deleteCache(higher, rm1Vec[j]);
		}
	}
	//std::cout<<"finished"<<lower<<"to:"<<higher<<std::endl;
	return 1;
}
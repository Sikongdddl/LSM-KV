#include <iostream>
#include <stdlib.h>

#include "skiplist.h"

double SkipList::my_rand()
{
    s = (16807 * s) % 2147483647ULL;
    return (s + 0.0) / 2147483647ULL;
}

int SkipList::randomLevel()
{
    int result = 1;
    while (result < MAX_LEVEL && my_rand() < 0.36787944117)
    {
        ++result;
    }
    return result;
}

bool SkipList::Insert(uint64_t key, std::string value)
{
    std::vector <SKNode*> update;
    for (int i = 0; i < MAX_LEVEL; ++i)
    {
        update.push_back(nullptr);
    }

    SKNode* searchTmp = head;
    for(int i=MAX_LEVEL-1;i>=0;i--)
    {
        while(searchTmp->forwards[i]->key < key)
        {
            searchTmp = searchTmp->forwards[i];
        }
        update[i]=searchTmp;
    }
    searchTmp = searchTmp->forwards[0];

    if(searchTmp->key == key)//update
    {
        searchTmp->val = value;
        return 0;
    }

    else//insert
    {
        int newLevel = randomLevel()-1;
        searchTmp = new SKNode(key,value,NORMAL);
        for(int i=0;i<=newLevel;i++)
        {
            searchTmp->forwards[i]=update[i]->forwards[i];
            update[i]->forwards[i] = searchTmp;
        }
        return 1;
    }
}

std::string SkipList::Search(uint64_t key)
{
    SKNode* searchTmp = head;
    for(int i=MAX_LEVEL-1;i>=0;i--)
    {
        while(searchTmp->forwards[i]->key < key )
        {
            //std::cout<<i+1<<",";
            //if(searchTmp->type == 1) std::cout<<"h ";
            //else if(searchTmp->type ==2) std::cout<<searchTmp->key<<" ";
            //else if(searchTmp->type ==3) std::cout<<"N ";
            searchTmp = searchTmp->forwards[i];
        }
        //std::cout<<i+1<<",";
        //if(searchTmp->type == 1) std::cout<<"h ";
        //else if(searchTmp->type ==2) std::cout<<searchTmp->key<<" ";
        //else if(searchTmp->type ==3) std::cout<<"N ";
    }
    searchTmp = searchTmp->forwards[0];
    //std::cout<<1<<",";
    //if(searchTmp->type == 1) std::cout<<"h ";
    //else if(searchTmp->type ==2) std::cout<<searchTmp->key<<" ";
    //else if(searchTmp->type ==3) std::cout<<"N ";

    if(searchTmp->key == key)
    {
        //std::cout<<searchTmp->val<<std::endl;  
        return searchTmp->val;
    }

    else 
    {
        //std::cout<<"Not Found"<<std::endl;
        return "";
    }
    
}

bool SkipList::Delete(uint64_t key)
{
    std::vector<SKNode*> update;
    for (int i = 0; i < MAX_LEVEL; ++i)
    {
        update.push_back(nullptr);
    }

    SKNode* searchTmp = head;
    for(int i = MAX_LEVEL-1;i>=0;i--)
    {
        while(searchTmp->forwards[i]->key < key)
        {
            searchTmp = searchTmp->forwards[i];
        }
        update[i] = searchTmp;
    }
    searchTmp = searchTmp->forwards[0];

    if(searchTmp->key == key)
    {
        for(int i = 0;i<=MAX_LEVEL-1;i++)
        {
            if(update[i]->forwards[i]!=searchTmp) break;

            update[i]->forwards[i] = searchTmp->forwards[i];
        }
        delete searchTmp;
        return true;
    }
    else return false;
}
#include <fstream>
#include <iostream>
#include <map>
#include <string>
#include <vector>
//---------------------------------------------------------------------------
// RDF-3X
// (c) 2008 Thomas Neumann. Web site: http://www.mpi-inf.mpg.de/~neumann/rdf3x
//
// This work is licensed under the Creative Commons
// Attribution-Noncommercial-Share Alike 3.0 Unported License. To view a copy
// of this license, visit http://creativecommons.org/licenses/by-nc-sa/3.0/
// or send a letter to Creative Commons, 171 Second Street, Suite 300,
// San Francisco, California, 94105, USA.
//---------------------------------------------------------------------------
using namespace std;
//---------------------------------------------------------------------------
static bool readManifest(const char* fileName,map<string,vector<string> >& predicates)
   // Read and parse a manifest file
{
   ifstream in(fileName);
   if (!in.is_open()) {
      cout << "unable to open " << fileName << endl;
      return false;
   }

   while (true) {
      string s;
      getline(in,s);
      if (!in.good()) break;
      while (s.length()&&((s[s.length()-1]=='\n')||(s[s.length()-1]=='\r')))
         s.resize(s.length()-1);

      string predicate=s.substr(0,s.find('/'));
      predicates[predicate].push_back(s);
   }

   return true;
}
//---------------------------------------------------------------------------
static unsigned mapString(map<string,unsigned>& stringMap,const string& s)
   // Map a string to an id
{
   // Trim the string
   string::const_iterator begin=s.begin(),limit=s.end();
   if ((begin!=limit)&&((begin+1)!=limit)&&((*begin)=='\"')&&((*(limit-1))=='\"')) {
      begin++;
      limit--;
   }
   while ((begin!=limit)&&((*begin)==' ')) begin++;
   while ((begin!=limit)&&((*(limit-1))==' ')) limit--;
   string key=string(begin,limit);

   if (stringMap.count(key)) {
      return stringMap[key];
   } else {
      unsigned result=stringMap.size();
      stringMap[key]=result;
      return result;
   }
}
//---------------------------------------------------------------------------
static void dumpFile(ofstream& out,map<string,unsigned>& stringMap,const string& fileName,unsigned predicate)
   // Dump a yago file into the facts table
{
   ifstream in(fileName.c_str());
   if (!in.is_open()) {
      cout << "warning: unable to open " << fileName << endl;
      return;
   }

   string s;
   while (true) {
      getline(in,s);
      if (!in.good()) break;

      // Parse the entries
      string::const_iterator iter=s.begin(),limit=s.end();
      while ((iter!=limit)&&((*iter)!='\t')) iter++;
      string::const_iterator sep1=iter;
      if (iter!=limit) ++iter;
      while ((iter!=limit)&&((*iter)!='\t')) iter++;
      string::const_iterator sep2=iter;
      if (iter!=limit) ++iter;
      while ((iter!=limit)&&((*iter)!='\t')) iter++;
      string::const_iterator sep3=iter;

      // Invalid parsing?
      if (sep2==sep3) {
         cout << "warning: invalid line '" << s << "' in " << fileName << endl;
         continue;
      }

      // Get the entries
      string subject=string(sep1+1,sep2);
      string object=string(sep2+1,sep3);

      // And write
      out << mapString(stringMap,subject) << "\t" << predicate << "\t" << mapString(stringMap,object) << endl;
   }
}
//---------------------------------------------------------------------------
int main()
{
   // Read the manifest
   map<string,vector<string> > predicates;
   if (!readManifest("manifest",predicates))
      return 1;

   // Map all predicates first
   map<string,unsigned> stringMap;
   for (map<string,vector<string> >::const_iterator iter=predicates.begin(),limit=predicates.end();iter!=limit;++iter) {
      mapString(stringMap,(*iter).first);
   }

   // Create the facts table
   {
      ofstream out("facts");
      for (map<string,vector<string> >::const_iterator iter=predicates.begin(),limit=predicates.end();iter!=limit;++iter) {
         unsigned predicate=mapString(stringMap,(*iter).first);
         for (vector<string>::const_iterator iter2=(*iter).second.begin(),limit2=(*iter).second.end();iter2!=limit2;++iter2)
            dumpFile(out,stringMap,*iter2,predicate);
      }
   }

   // Create the strings table
   {
      ofstream out("strings");
      for (map<string,unsigned>::const_iterator iter=stringMap.begin(),limit=stringMap.end();iter!=limit;++iter)
         out << (*iter).second << " " << (*iter).first << endl;
   }
}
//---------------------------------------------------------------------------

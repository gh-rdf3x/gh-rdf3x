#include <set>
#include <vector>
#include <string>
#include <cstring>
#include <cstdio>
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
namespace std { };
using namespace std;
//---------------------------------------------------------------------------
#if defined(__WIN32__)||defined(WIN32)
static const char SEP = '\\';
static const bool specialRoot = false;
static bool isDriveRelative(const string& path)
{
   string::size_type len=path.length();
   if (len<2) return false;

   char c=path[0];
   if ((((c>='a')&&(c<='z'))||((c>='A')&&(c<='Z')))&&(path[1]==':')) {
      if (len<3) return true;
      if ((path[2]=='\\')||(path[2]=='/')) return false;
      return true;
   } else return false;
}
static string mapDriveRelative(const string& str) { return str; } // XXX
#else
static const char SEP = '/';
static const bool specialRoot = false; // Is / different than //?
static bool isDriveRelative(const string&) { return false; }
static string mapDriveRelative(const string& str) { return str; }
#endif
//---------------------------------------------------------------------------
static string normalize(const string& filePath)
{
   // A quick scan first...
   bool hadSep=false,special=isDriveRelative(filePath),needsFix=special;
   string::size_type len=filePath.length();
   if (!needsFix)
   for (string::size_type index=0;index<len;index++) {
      char c=filePath[index];
      if (c==SEP) { needsFix=(c!='/'); c='/'; }
      if (c=='/') {
         if (hadSep&&((!specialRoot)||(index>1)))
            needsFix=true;
         hadSep=true;
      } else {
         if (c=='.')
            if (hadSep||(index==0))
               needsFix=true;
         hadSep=false;
      }
   }
   if (!needsFix)
      return filePath;
   hadSep=false;
   // Construct the fixed result
   string result;
   if (special) result=mapDriveRelative(filePath);
   for (string::size_type index=0;index<len;index++) {
      char c=filePath[index];
      if (c==SEP) c='/';
      if (c=='/') {
         if (hadSep&&((!specialRoot)||(index>1))) {
         } else result+=c;
         hadSep=true;
      } else {
         if ((c=='.')&&(hadSep||(index==0))) {
            if (index+1>=len) {
               if (hadSep) result.resize(result.length()-1);
               continue;
            }
            char n=filePath[index+1];
            if ((n=='/')||(n==SEP)) {
               index++; continue;
            }
            if (n=='.') {
               if (index+2>=len) {
                  index++;
                  string::size_type split=result.rfind('/',result.length()-2);
                  if (split!=string::npos) {
                     if (result.substr(split)!="/../")
                        result.resize(split);
                  } else if (result.length()>0) {
                     if ((result!="../")&&(result!="/")) result.clear();
                  } else result="..";
                  continue;
               } else {
                  n=filePath[index+2];
                  if ((n=='/')||(n==SEP)) {
                     index+=2;
                     string::size_type split=result.rfind('/',result.length()-2);
                     if (split!=string::npos) {
                        if (result.substr(split)!="/../")
                           result.resize(split+1);
                     } else if (result.length()>0) {
                        if ((result!="../")&&(result!="/")) result.clear();
                     } else result="../";
                     continue;
                  }
               }
            }
         }
         result+=c; hadSep=false;
      }
   }
   return result;
}
//---------------------------------------------------------------------------
static string localName(const string& str)
{
   string result=str;
   for (string::iterator iter=result.begin();iter!=result.end();++iter)
      if ((*iter)=='/')
         *iter=SEP;
   return result;
}
//---------------------------------------------------------------------------
static bool exists(const string& name)
{
   string local=localName(name);
   FILE* in=fopen(local.c_str(),"rt");
   if (!in) return false;
   fclose(in);

   return true;
}
//---------------------------------------------------------------------------
static string couldBuild(const string& name,const vector<string>& includePath,const set<string>& generated)
{
   for (vector<string>::const_iterator iter=includePath.begin();iter!=includePath.end();++iter) {
      string n=normalize((*iter)+'/'+name);
      if (generated.count(n)) {
         return n;
      }
   }
   return "";
}
//---------------------------------------------------------------------------
static bool collect(const string& name,set<string>& headers,set<string>& missing,const vector<string>& includePath,const set<string>& generated)
{
   FILE* in=fopen(localName(name).c_str(),"rt");
   if (!in)
      return headers.size();

   char buffer[8192];
   while (true) {
      if (!fgets(buffer,sizeof(buffer),in)) break;
      char* index=buffer;
      while ((*index==' ')||(*index=='\t')) index++;
      if (strncmp(index,"#include",8)!=0) continue;
      index+=8;
      while ((*index==' ')||(*index=='\t')) index++;
      bool checkLocal=false;
      if (index[0]=='\"') {
         index++;
         if (!strstr(index,"\"")) continue;
         *strstr(index,"\"")=0;
         checkLocal=true;
      } else if (index[0]=='<') {
         index++;
         if (!strstr(index,">")) continue;
         *strstr(index,">")=0;
      } else continue;

      // Check for the file
      if (checkLocal) {
         string prefix;
         if (name.rfind('/')!=string::npos)
            prefix=name.substr(0,name.rfind('/')+1);
         string fileName=normalize(prefix+index);
         if (exists(fileName)) {
            if (headers.count(fileName))
               continue;
            headers.insert(fileName);
            if (!collect(fileName,headers,missing,includePath,generated))
               return false;
         }
         if (missing.count(fileName))
            continue;
      }
      { bool found=false;
      for (vector<string>::const_iterator iter=includePath.begin();iter!=includePath.end();++iter) {
         string fileName=normalize((*iter)+"/"+index);
         if (exists(fileName)) {
            found=true;
            if (headers.count(fileName))
               break;
            headers.insert(fileName);
            if (!collect(fileName,headers,missing,includePath,generated))
               return false;
            break;
         }
         if (missing.count(fileName)) {
            found=true;
            break;
         }
      }
      if (found) continue; }

      // Could it be generated?
      if (checkLocal) {
         string fileName=normalize(index);
         if (generated.count(fileName)) {
            headers.insert(fileName);
            missing.insert(fileName);
            continue;
         }
      }
      string fileName=couldBuild(index,includePath,generated);
      if (name!="") {
         headers.insert(fileName);
         missing.insert(fileName);
      }
   }

   fclose(in);
   return true;
}
//---------------------------------------------------------------------------
int main(int argc, char* argv[])
{
   if (argc<3) {
      fprintf(stderr,"Usage: getdep [options] input object [generated files]\n");
      return 3;
   }
   string         targetName,objectName,sourceName;
   vector<string> includePath;
   set<string>    generated;
   bool           argsOnly=false;
   for (int index=1;index<argc;index++) {
      if (argsOnly||((argv[index][0]!='-')&&(argv[index][0]!='/'))) {
         if (sourceName!="") {
            if (objectName!="") {
               generated.insert(normalize(argv[index]));
            } else {
               objectName=normalize(argv[index]);
            }
         } else {
            sourceName=normalize(argv[index]);
         }
      } else {
         if ((argv[index][1]=='-')&&(!argv[index][2])) { argsOnly=true; continue; }
         if (argv[index][1]=='o') {
            if (argv[index][2])
               targetName=normalize(argv[index]+2); else
               targetName=normalize(argv[++index]);
         } else if (argv[index][1]=='I') {
            if (argv[index][2])
               includePath.push_back(normalize(argv[index]+2)); else
               includePath.push_back(normalize(argv[++index]));
         } else {
            fprintf(stderr,"unknown option '%s'\n",argv[index]);
            return 1;
         }
      }
   }
   if (sourceName=="") {
      fprintf(stderr,"No source name specified\n");
      return 1;
   }
   if (objectName=="") {
      fprintf(stderr,"No object name specified\n");
      return 1;
   }
   if (targetName=="") {
      targetName=sourceName+".d";
   }
   // Collect the dependencies
   set<string> headers,missing;
   if (!collect(sourceName,headers,missing,includePath,generated))
      return 1;


   // Write the result
   FILE* out=fopen(localName(targetName).c_str(),"wt");
   if (!out) {
      fprintf(stderr,"Unable to create %s\n",targetName.c_str());
      return 3;
   } else {
      fprintf(out,"%s: %s",objectName.c_str(),sourceName.c_str());
      for (set<string>::const_iterator iter=headers.begin();iter!=headers.end();++iter)
         fprintf(out," %s",(*iter).c_str());
      fprintf(out,"\n\n");

      fprintf(out,"%s: %s",targetName.c_str(),sourceName.c_str());
//      for (set<string>::const_iterator iter=missing.begin();iter!=missing.end();++iter)
//         fprintf(out," %s",(*iter).c_str());
      fprintf(out," $(wildcard");
      for (set<string>::const_iterator iter=headers.begin();iter!=headers.end();++iter)
//         if (!missing.count(*iter))
            fprintf(out," %s",(*iter).c_str());
      fprintf(out,")\n\n");
      fclose(out);
   }

   return 0;
}
//---------------------------------------------------------------------------



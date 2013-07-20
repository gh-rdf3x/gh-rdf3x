#include <string>
#include <iostream>
#include <fstream>
#include <cstdlib>
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
/// A benchmark driver
class Driver
{
   private:
   /// The nuber of passes
   static const unsigned passes = 5;

   protected:
   /// Restart the database
   virtual void restartDB(const string& db,bool cold) = 0;
   /// Run a query
   virtual double timeQuery(const string& db,const string& query) = 0;

   /// Flush the file system buffers
   void flushCaches();

   public:
   /// Constructor
   Driver();
   /// Destructor
   virtual ~Driver();

   /// Get cold runtimes
   double coldTimes(const string& db,const string& query);
   /// Get warm runtimes
   double warmTimes(const string& db,const string& query);
};
//---------------------------------------------------------------------------
/// Driver for RDF 3X
class DriverRDF3X : public Driver
{
   public:
   /// Restart the database
   void restartDB(const string& db,bool cold);
   /// Run a query
   double timeQuery(const string& db,const string& query);
};
//---------------------------------------------------------------------------
Driver::Driver()
   // Constructor
{
}
//---------------------------------------------------------------------------
Driver::~Driver()
   // Destructor
{
}
//---------------------------------------------------------------------------
void Driver::flushCaches()
   // Flush the operating system caches
{
   if (system("sudo ~/bin/flushcaches")!=0)
      cerr << "warning: flushing caches failed!" << std::endl;
}
//---------------------------------------------------------------------------
double Driver::coldTimes(const string& db,const string& query)
   // Get cold runtimes
{
   double result=0;

   for (unsigned index=0;index<passes;index++) {
      restartDB(db,true);
      flushCaches();

      double t=timeQuery(db,query);
      if ((!index)||(t<result))
         result=t;
   }

   return result;
}
//---------------------------------------------------------------------------
double Driver::warmTimes(const string& db,const string& query)
   // Get warm runtimes
{
   double result=0;

   for (unsigned index=0;index<passes;index++) {
      restartDB(db,false);

      double t=timeQuery(db,query);
      if ((!index)||(t<result))
         result=t;
   }

   return result;
}
//---------------------------------------------------------------------------
void DriverRDF3X::restartDB(const string& /*db*/,bool /*cold*/)
   // Restart the database
{
   // Nothing to be done here, the system is restarted for each query anyway
}
//---------------------------------------------------------------------------
double DriverRDF3X::timeQuery(const string& db,const string& query)
   // Run a query
{
   string command="bin/evalsparql "+db+" "+query+" >.runbenchmark.out";
   if (system(command.c_str())!=0) {
      cerr << "error: executing " << command << " failed!" << endl;
      return -1;
   }

   double result=-1;
   {
      ifstream in(".runbenchmark.out");
      string s;
      while (getline(in,s)) {
         if (s.substr(0,16)=="Execution time: ") {
            s=s.substr(16);
            s=s.substr(0,s.find(' '));
            int i=atoi(s.c_str());
            if ((i==0)&&(s!="0"))
               continue;
            double t=static_cast<double>(i)/1000.0;
            if ((result<0)||(t<result))
               result=t;
         }
      }
   }
   remove(".runbenchmark.out");
   if (result<0)
      cerr << "error: unable to extract the execution time" << endl;
   return result;
}
//---------------------------------------------------------------------------
static string readLine(ifstream& in)
   // Read a line from a file
{
   string s;
   while (getline(in,s)) {
      // Trim and skip comments
      if ((s.length()>0)&&((s[0]==' ')||(s[0]=='\t')))
         s=s.substr(1);
      if ((s.length()>0)&&(s[0]=='#')) continue;
      if ((s.length()>0)&&((s[s.length()-1]==' ')||(s[s.length()-1]=='\t')))
         s=s.substr(0,s.length()-1);

      // Got a non-empty line?
      if (s.length())
         return s;
   }
   return "";
}
//---------------------------------------------------------------------------
int main(int argc,char* argv[])
{
   // Check the arguments
   if (argc<2) {
      cerr << "usage: " << argv[0] << " <benchmark>" << std::endl;
      return 1;
   }

   // Open the description
   ifstream in(argv[1]);
   if (!in.is_open()) {
      cerr << "unable to open " << argv[1] << std::endl;
      return 1;
   }

   // Check the driver
   string engine=readLine(in);
   Driver* driver;
   if (engine=="rdf-3x") {
      driver=new DriverRDF3X();
   } else {
      cerr << "unknown benchmark driver " << engine << std::endl;
      return 1;
   }
   string db=readLine(in);

   // Run the queries
   while (true) {
      // Parse the next query statement
      string query=readLine(in);
      if (!query.length()) break;
      if (query.find(' ')==string::npos) {
         cerr << "warning: malformed query line " << query << endl;
         continue;
      }
      string queryName=query.substr(0,query.find(' '));
      string queryFile=query.substr(query.find(' ')+1);

      // Run the query
      cout << queryName; cout.flush();
      double coldTimes=driver->coldTimes(db,queryFile);
      cout << " " << coldTimes; cout.flush();
      double warmTimes=driver->warmTimes(db,queryFile);
      cout << " " << warmTimes << endl;
   }

   // Cleanup
   delete driver;
}
//---------------------------------------------------------------------------

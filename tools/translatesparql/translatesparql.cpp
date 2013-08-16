#include "cts/infra/QueryGraph.hpp"
#include "cts/semana/SemanticAnalysis.hpp"
#include "cts/parser/SPARQLLexer.hpp"
#include "cts/parser/SPARQLParser.hpp"
#include "rts/database/Database.hpp"
#include <iostream>
#include <fstream>
#include <cstdlib>
#include <map>
using namespace std;

//---------------------------------------------------------------------------
// RDF-3X
// Created by: 
//         Thomas Neumann. Web site: http://www.mpi-inf.mpg.de/~neumann/rdf3x
//         (c) 2008 
//
// This work is licensed under the Creative Commons
// Attribution-Noncommercial-Share Alike 3.0 Unported License. To view a copy
// of this license, visit http://creativecommons.org/licenses/by-nc-sa/3.0/
// or send a letter to Creative Commons, 171 Second Street, Suite 300,
// San Francisco, California, 94105, USA.
// 
//  -----------------------------------------------------------------------
//
// Modified by:
//         Giuseppe De Simone and Hancel Gonzalez
//         Advisor: Maria Esther Vidal
//         
// Universidad Simon Bolivar
// 2013,   Caracas - Venezuela.
//         
// This is our modification to the original translatesparql tool
// In this version we modified some functions and add some operators
// now it can translate operators as OPTIONAL, UNION, GJOIN and some FILTERS
// for both engines (PostgreSQL and MonetDB)
//---------------------------------------------------------------------------

static string readInput(istream& in) {
// Read the input query 
  string result;
  while (true) {
    string s;
    getline(in,s);
    if (!in.good())
      break;
    result+=s;
    result+='\n';
  }
  return result;
}

//---------------------------------------------------------------------------
static string buildFactsAttribute(unsigned id,const char* attribute) {
// Build the attribute name for a facts attribute
  char buffer[50];
  snprintf(buffer,sizeof(buffer),"f%u.%s",id,attribute);
  return string(buffer);
}
//---------------------------------------------------------------------------
static string databaseName(const char* fileName) {
// Guess the database name from the file name
  const char* start=fileName;
  for (const char* iter=fileName;*iter;++iter)
    if ((*iter)=='/')
      start=iter+1;
  const char* stop=start;
  while ((*stop)&&((*stop)!='.'))
    ++stop;
  return string(start,stop);
}

//---------------------------------------------------------------------------
// Name: set2map
// Created by: Giuseppe De Simone and Hancel Gonzalez
// Advisor: Maria Esther Vidal
// Description: Auxiliar function used for translatePostgres 
//---------------------------------------------------------------------------
static void set2map(set<unsigned> var, map<unsigned,unsigned>& var2pos) {
  unsigned i=0;
  for (set<unsigned>::const_iterator iter=var.begin(),limit=var.end();iter!=limit;++iter) {
    var2pos[*iter] = i;
    i++;
  }
}

//---------------------------------------------------------------------------
// Name: getVariables
// Created by: Giuseppe De Simone and Hancel Gonzalez
// Advisor: Maria Esther Vidal
// Description: Get variables for the filters 
//---------------------------------------------------------------------------
static void getVariables(QueryGraph::Filter filter, set<unsigned>& vars) {
  if (filter.type == QueryGraph::Filter::Variable)
    vars.insert(filter.id);
  else if ((filter.type == QueryGraph::Filter::Or) || (filter.type ==  QueryGraph::Filter::And) || 
           (filter.type == QueryGraph::Filter::Equal) || (filter.type == QueryGraph::Filter::NotEqual) || 
           (filter.type == QueryGraph::Filter::Less) || (filter.type == QueryGraph::Filter::LessOrEqual)|| 
           (filter.type == QueryGraph::Filter::Greater) || (filter.type == QueryGraph::Filter::GreaterOrEqual) || 
           (filter.type == QueryGraph::Filter::Plus) || (filter.type == QueryGraph::Filter::Minus) || 
           (filter.type == QueryGraph::Filter::Mul) || (filter.type == QueryGraph::Filter::Div) || 
           (filter.type == QueryGraph::Filter::Builtin_regex)) {
    getVariables(*filter.arg1,vars);
    getVariables(*filter.arg2,vars);
  }
  else if ((filter.type == QueryGraph::Filter::Not && filter.arg1->type != QueryGraph::Filter::Builtin_bound) || 
           (filter.type == QueryGraph::Filter::UnaryPlus) || 
           (filter.type == QueryGraph::Filter::UnaryMinus))
    getVariables(*filter.arg1,vars);

}


static void getVariables(vector<QueryGraph::Filter> filters, set<unsigned>& vars) {
  for (vector<QueryGraph::Filter>::const_iterator iter=filters.begin(),limit=filters.end();iter!=limit;++iter)
      getVariables(*iter,vars);
}


static void getVariables(QueryGraph::SubQuery subquery, set<unsigned>& vars) {
   //Get variables of Basic Group Graph Pattern
   for (vector<QueryGraph::Node>::const_iterator iter=subquery.nodes.begin(),limit=subquery.nodes.end();iter!=limit;++iter) {
      if ((!(*iter).constSubject))
        vars.insert((*iter).subject);
      if ((!(*iter).constPredicate))
        vars.insert((*iter).predicate);
      if ((!(*iter).constObject))
        vars.insert((*iter).object);
    }
}

static void getVariables(vector<QueryGraph::SubQuery> subqueries, set<unsigned>& vars) {
  //Get variables of a vector of Basic Group Graph Pattern
  for (vector<QueryGraph::SubQuery>::iterator iter1=subqueries.begin() , limit1=subqueries.end() ; iter1!=limit1 ; ++iter1)
    getVariables(*iter1,vars);
}


//---------------------------------------------------------------------------
// Name: getOptionalsVariables
// Authors: Giuseppe De Simone and Hancel Gonzalez
// Advisor: Maria Esther Vidal
// Description: Get variables of a vector of Basic Group Graph Pattern
//              and Optional Group Graph Pattern  
//---------------------------------------------------------------------------
static void getOptionalsVariables(QueryGraph::SubQuery subquery, set<unsigned>& vars) {
   //Get variables of Basic Group Graph Pattern and Optional Group Graph Pattern
   for (vector<QueryGraph::Node>::const_iterator iter=subquery.nodes.begin(),limit=subquery.nodes.end();iter!=limit;++iter) {
      if ((!(*iter).constSubject))
        vars.insert((*iter).subject);
      if ((!(*iter).constPredicate))
        vars.insert((*iter).predicate);
      if ((!(*iter).constObject))
        vars.insert((*iter).object);
   }
   if(subquery.optional.size())
     getVariables(subquery.optional,vars);
}


//---------------------------------------------------------------------------
// Name: getOptionalsVariables
// Authors: Giuseppe De Simone and Hancel Gonzalez
// Advisor: Maria Esther Vidal
// Description: Get variables of a vector of Basic Group Graph Pattern 
//---------------------------------------------------------------------------
static void getOptionalsVariables(vector<QueryGraph::SubQuery> subqueries, set<unsigned>& vars) {
  //Get variables of a vector of Basic Group Graph Pattern
  for (vector<QueryGraph::SubQuery>::iterator iter1=subqueries.begin() , limit1=subqueries.end() ; iter1!=limit1 ; ++iter1)
    getOptionalsVariables(*iter1,vars);
}

//---------------------------------------------------------------------------
// Name: join
// Authors: Giuseppe De Simone and Hancel Gonzalez
// Advisor: Maria Esther Vidal
// Description: Join the sets set1 and set2,
//              the union is saved in the set join
//--------------------------------------------------------------------------
static void join(set<unsigned> set1, set<unsigned> set2, set<unsigned>& join) {
    //Join two sets and storage union in another set variable
    for(set<unsigned>::iterator iter=set2.begin(),limit=set2.end();iter!=limit;++iter)
        join.insert(*iter);
    for(set<unsigned>::iterator iter=set1.begin(),limit=set1.end();iter!=limit;++iter)
        join.insert(*iter);
}

//---------------------------------------------------------------------------
// Name: intersect
// Authors: Giuseppe De Simone and Hancel Gonzalez
// Advisor: Maria Esther Vidal
// Description: Intersect the sets set1 and set2,
//              the intersection is saved in the set intersect 
//--------------------------------------------------------------------------
static void intersect(set<unsigned> set1, set<unsigned> set2, set<unsigned>& intersect) {
    //Intersect two sets and storage intersection in another set variable
    for(set<unsigned>::iterator iter=set2.begin(),limit=set2.end();iter!=limit;++iter)
      if(set1.count(*iter))
        intersect.insert(*iter);
    for(set<unsigned>::iterator iter=set1.begin(),limit=set1.end();iter!=limit;++iter)
      if(set2.count(*iter))
        intersect.insert(*iter);
}

//---------------------------------------------------------------------------
// Name: difference
// Authors: Giuseppe De Simone and Hancel Gonzalez
// Advisor: Maria Esther Vidal
// Description: Erase the elements in set1 that belongs also to set2 
//              
//---------------------------------------------------------------------------
static void difference(set<unsigned>& set1, set<unsigned> set2) {
    //Erase elements in set2 on set1
    for(set<unsigned>::iterator iter=set2.begin(),limit=set2.end();iter!=limit;++iter)
      if(set1.count(*iter))
        set1.erase(*iter);
}

//---------------------------------------------------------------------------
// Name: onOptional
// Authors: Giuseppe De Simone and Hancel Gonzalez
// Advisor: Maria Esther Vidal
// Description: Procedure use to build the on attributes for the optional 
//              clause. It's used for the translation to postgres and 
//              also for the translation to MonetDB 
//----------------------------------------------------------------------------
static void onOptional(set<unsigned> commons, unsigned fact_ini, unsigned fact) {       
  cout << "facts" << fact << "\n    ON (";
  unsigned j = 0;
  for (set<unsigned>::iterator iter1=commons.begin(),limit1=commons.end();iter1!=limit1;++iter1){
    if (j) cout << " and "; 
    cout << "facts" << fact_ini << ".p" << j << " = facts" << fact << ".q" << j;
    j++;
  }
  cout << ")";
}

//---------------------------------------------------------------------------
// Name: pattern
// Authors: Giuseppe De Simone and Hancel Gonzalez
// Advisor: Maria Esther Vidal
// Description: Structure used to save variables of queries with GJOIN clause 
//----------------------------------------------------------------------------
struct pattern {
	struct variables {
		set<unsigned> vars, commons;

		variables() {
			vars.clear();
			commons.clear();
		}

		~variables() {
		}
	};

	public:
	variables *vars;
	pattern *arg1, *arg2;

	pattern(){
		vars = new variables;
		arg1 = NULL;
		arg2 = NULL;
	}
};

//---------------------------------------------------------------------------
// Name: query2structure
// Authors: Giuseppe De Simone and Hancel Gonzalez
// Advisor: Maria Esther Vidal
// Description: Given a QueryGraph with GJOIN clause, it get the variables 
//              and save this information in the structure.
//----------------------------------------------------------------------------
static void query2structure(QueryGraph::SubQuery query, pattern *structure) {
	if (query.gjoins.empty()) {
		getVariables(query,structure->vars->vars);
	}
	else {
		structure->arg1 = new pattern;
		structure->arg2 = new pattern;
		query2structure(query.gjoins[0][0],structure->arg1);
		query2structure(query.gjoins[0][1],structure->arg2);
		join(structure->arg1->vars->vars,structure->arg2->vars->vars,structure->vars->vars);
		intersect(structure->arg1->vars->vars,structure->arg2->vars->vars,structure->vars->commons);
	}
}

//---------------------------------------------------------------------------
// Name: translateFilterPostgres
// Authors: Giuseppe De Simone and Hancel Gonzalez
// Advisor: Maria Esther Vidal
// Description: Make the translation for all the filters that  has to 
//              be translate to the schema of Postgres 
//--------------------------------------------------------------------------- 
static void translateFilterPostgres(QueryGraph::Filter filter, map<unsigned,unsigned> var) {
  map<QueryGraph::Filter::Type,string> binaryOperator; 
  binaryOperator[QueryGraph::Filter::Or]             = "or";
  binaryOperator[QueryGraph::Filter::And]            = "and";
  binaryOperator[QueryGraph::Filter::Equal]          = "=";
  binaryOperator[QueryGraph::Filter::NotEqual]       = "<>";
  binaryOperator[QueryGraph::Filter::Less]           = "<";
  binaryOperator[QueryGraph::Filter::LessOrEqual]    = "<=";
  binaryOperator[QueryGraph::Filter::Greater]        = ">";
  binaryOperator[QueryGraph::Filter::GreaterOrEqual] = ">=";
  binaryOperator[QueryGraph::Filter::Plus]           = "+";
  binaryOperator[QueryGraph::Filter::Minus]          = "-";
  binaryOperator[QueryGraph::Filter::Mul]            = "*";
  binaryOperator[QueryGraph::Filter::Div]            = "/";
  binaryOperator[QueryGraph::Filter::Builtin_regex]  = "like";

  map<QueryGraph::Filter::Type,string> unaryOperator;
  unaryOperator[QueryGraph::Filter::Not]        = "not";
  unaryOperator[QueryGraph::Filter::UnaryPlus]  = "+";
  unaryOperator[QueryGraph::Filter::UnaryMinus] = "-"; 
  
  if (filter.type == QueryGraph::Filter::Variable) 
    cout << "m" << var[filter.id] << ".value::int"; //We forced cast to INT type
  else if (filter.type == QueryGraph::Filter::Literal)
    cout << filter.value;
  else if (filter.type == QueryGraph::Filter::IRI)
    cout << "\'" << filter.value << "\'";
  else if (unaryOperator.count(filter.type)) {
   if(filter.arg1->type != QueryGraph::Filter::Builtin_bound) {
    cout << unaryOperator[filter.type] << "(";
    translateFilterPostgres(*filter.arg1,var);
    cout << ")";
   }
  }
  else if (binaryOperator.count(filter.type)){
    cout << "(";
    translateFilterPostgres(*filter.arg1,var);
    cout << " " << binaryOperator[filter.type] << " ";
    if(filter.type == QueryGraph::Filter::Builtin_regex) {
      cout << "'%";
      translateFilterPostgres(*filter.arg2,var);
      cout << "%'";
    }
    else
      translateFilterPostgres(*filter.arg2,var);
    cout << ")";
  }
} 

//---------------------------------------------------------------------------
// Name: translateSubQueryPostgres
// Modified by: Giuseppe De Simone and Hancel Gonzalez
// Advisor: Maria Esther Vidal
// Description: For a given QueryGraph with no optionals or unions
//              make the translation to the schema of
//              Postgres
//---------------------------------------------------------------------------
static void translateSubQueryPostgres(QueryGraph& query, QueryGraph::SubQuery subquery, map<unsigned,unsigned>& projection, const string& schema, set<unsigned> unionvars, map<unsigned,unsigned>& null) {
  //Create dictionary with elements of nodes.
  map<unsigned,string> representative;
  {
    unsigned id=0;
    for (vector<QueryGraph::Node>::const_iterator iter=subquery.nodes.begin(),limit=subquery.nodes.end();iter!=limit;++iter) {
      if ((!(*iter).constSubject)&&(!representative.count((*iter).subject)))
	representative[(*iter).subject]=buildFactsAttribute(id,"subject");
      if ((!(*iter).constPredicate)&&(!representative.count((*iter).predicate)))
	representative[(*iter).predicate]=buildFactsAttribute(id,"predicate");
      if ((!(*iter).constObject)&&(!representative.count((*iter).object)))
	representative[(*iter).object]=buildFactsAttribute(id,"object");
      ++id;
    }
  }
  //Translate SELECT clause but values are id's. 
  cout << "   (select ";
  {
    unsigned id=0, i=0;
    for (QueryGraph::projection_iterator iter=query.projectionBegin(),limit=query.projectionEnd();iter!=limit;++iter) { 
      if (i) cout << ",";
      //Translate projection variables without repeat it.
      if (!projection.count(*iter)) {
        if (representative.count(*iter)) {
	  cout << representative[*iter];
          projection[*iter]=1;
          null[*iter]=1;
        }
        //Translate to NULL when UNION is translated.
        else if (!representative.count(*iter))// && null.count(*iter))
          cout << "NULL";
        
        cout << " as r" << id;
        i=1;
      }
      else {
        if (representative.count(*iter)) {
          cout << representative[*iter] << " as r" << id;
          i=1;
        }
        else if (null.count(*iter)) {
           cout << "NULL as r" << id;
           i=1;
        }
      }
      id++;
    }
    id=0;
    //Projection variables for JOIN when OPTIONAL is translated.
    for(set<unsigned>::iterator iter=unionvars.begin(),limit=unionvars.end();iter!=limit;++iter){
      if (i) cout << ","; 
      if (representative.count(*iter)) 
        cout << representative[*iter]; 
      else 
        cout << "NULL"; 
      cout << " as q" << id;
      i=1;
      id++;
    }
  }
  //One relation fx for node in the Group Graph Pattern
  cout << endl;
  cout << "    from ";
  {
    unsigned id=0;
    for (vector<QueryGraph::Node>::const_iterator iter=subquery.nodes.begin(),limit=subquery.nodes.end();iter!=limit;++iter) {
      if (id) cout << ",";
      cout << schema << ".facts f" << id;
      ++id;
    }

  }
  //Translate filters
  set<unsigned> varsfilters;
  if (subquery.filters.size()) {
    cout << ",";
    getVariables(subquery.filters,varsfilters);
    unsigned id=0;
    //Translate for get value of id
    for(set<unsigned>::const_iterator iter=varsfilters.begin(),limit=varsfilters.end();iter!=limit;iter++) {
      if(id) cout << ",";
      cout << schema << ".strings m" << id;
      id++;
    }
  }
  cout << endl;
  cout << "    where ";
  //Join conditions between nodes.
  {
    unsigned id=0; bool first=true;
    for (vector<QueryGraph::Node>::const_iterator iter=subquery.nodes.begin(),limit=subquery.nodes.end();iter!=limit;++iter) {
      string s=buildFactsAttribute(id,"subject"),p=buildFactsAttribute(id,"predicate"),o=buildFactsAttribute(id,"object");
      if ((*iter).constSubject) {
	if (first) first=false; else cout << " and ";
	cout << s << "=" << (*iter).subject;
      } else if (representative[(*iter).subject]!=s) {
	if (first) first=false; else cout << " and ";
	cout << s << "=" << representative[(*iter).subject];
      }
      if ((*iter).constPredicate) {
	if (first) first=false; else cout << " and ";
	cout << p << "=" << (*iter).predicate;
      } else if (representative[(*iter).predicate]!=p) {
	if (first) first=false; else cout << " and ";
	cout << p << "=" << representative[(*iter).predicate];
      }
      if ((*iter).constObject) {
	if (first) first=false; else cout << " and ";
	cout << o << "=" << (*iter).object;
      } else if (representative[(*iter).object]!=o) {
	if (first) first=false; else cout << " and ";
	cout << o << "=" << representative[(*iter).object];
      }
      ++id;
    }
  }
  //Join for get values for filters.
  if (subquery.filters.size()) {
    map<unsigned,unsigned> varsfilters_map;
    set2map(varsfilters,varsfilters_map);
    unsigned id = 0;
    for(set<unsigned>::const_iterator iter=varsfilters.begin(),limit=varsfilters.end();iter!=limit;iter++){
      cout << " and m" << id << ".id=" << representative[*iter];
      id++;
    }
    //Finally, translate filters.
    for(vector<QueryGraph::Filter>::iterator iter = subquery.filters.begin(), limit = subquery.filters.end() ; iter != limit ; iter++) {
      cout << " and "; 
      translateFilterPostgres(*iter,varsfilters_map);
    }
  }
  cout << ")";
}

//---------------------------------------------------------------------------
// Name: translateSubQueryOptionalPostgres
// Authors: Giuseppe De Simone and Hancel Gonzalez
// Advisor: Maria Esther Vidal
// Description: Build the body of the optional translation 
//---------------------------------------------------------------------------
static void translateSubQueryOptionalPostgres(QueryGraph& query, QueryGraph::SubQuery subquery, const string& schema,unsigned& f, unsigned& r, map<unsigned,unsigned>& projection, set<unsigned> optionalvars) {
  map<unsigned,string> representative;
  set<unsigned> vars1; 
  vector<set<unsigned> > vars2;
  set<unsigned> common;
  {
    unsigned id=f;
    //Create dictionary with elements of nodes.
    for (vector<QueryGraph::Node>::const_iterator iter=subquery.nodes.begin(),limit=subquery.nodes.end();iter!=limit;++iter) {
      if ((!(*iter).constSubject)&&(!representative.count((*iter).subject)))
	representative[(*iter).subject]=buildFactsAttribute(id,"subject");
      if ((!(*iter).constPredicate)&&(!representative.count((*iter).predicate)))
	representative[(*iter).predicate]=buildFactsAttribute(id,"predicate");
      if ((!(*iter).constObject)&&(!representative.count((*iter).object)))
	representative[(*iter).object]=buildFactsAttribute(id,"object");
      ++id;
    }
  }
  getVariables(subquery,vars1);
  
  //OPTIONAL inside this OPTIONAL clause? Get variables for JOIN
  if (subquery.optional.size()) {
    unsigned i=0;
    vars2.resize(subquery.optional.size());
    common.clear();
    for(vector<QueryGraph::SubQuery>::iterator iter=subquery.optional.begin(),limit=subquery.optional.end();iter!=limit;++iter) {
      getVariables(*iter,vars2[i]);
      intersect(vars1,vars2[i],common);
      i++;
      if ((*iter).unions.size() == 1) {
        getVariables((*iter).unions[0],vars2[0]);
        intersect(vars1,vars2[0],common);
      }
    }
  }
  
  cout << "   (select ";
  {
    unsigned id=r, tmp=0;
    for (QueryGraph::projection_iterator iter=query.projectionBegin(),limit=query.projectionEnd();iter!=limit;++iter) {
      //Don't repeat variables in SELECT clause
      if (representative.count(*iter) && !projection.count(*iter)) {
        projection[*iter] = 1;
        if (tmp) cout << ",";
        cout << representative[*iter] << " as r" << id;
        id++;
        tmp=1;
      }
    }
    r = id;
  }
  {
  unsigned id = 0;
  //Select variables from JOIN of OPTIONAL
  if (optionalvars.size())
    for(set<unsigned>::iterator iter=optionalvars.begin(),limit=optionalvars.end();iter!=limit;++iter){ 
      cout << "," << representative[*iter] << " as q" << id;
      id++;
    }

  id = 0;
  //Select variables to JOIN of OPTIONAL
  if (subquery.optional.size() || subquery.unions[0].size() == 1)
    for(set<unsigned>::iterator iter=common.begin(),limit=common.end();iter!=limit;++iter) {
      cout << "," << representative[*iter] << " as p" << id;
      id++;
    }
  }

  cout << endl;
  cout << "    from ";
  {
    unsigned id=f;
    for (vector<QueryGraph::Node>::const_iterator iter=subquery.nodes.begin(),limit=subquery.nodes.end();iter!=limit;++iter) {
      if (iter != subquery.nodes.begin()) cout << ",";
      cout << schema << ".facts f" << id;
      ++id;
    }   
  }

  set<unsigned> varsfilters;
  if (subquery.filters.size()) {
    cout << ",";
    getVariables(subquery.filters,varsfilters);
    unsigned id=0;
    for(set<unsigned>::const_iterator iter=varsfilters.begin(),limit=varsfilters.end();iter!=limit;iter++) {
      if(id) cout << ",";
      cout << schema << ".strings m" << id;
      id++;
    }
  }
 cout << endl;
  cout << "    where ";
  {
    unsigned id=f; bool first=true;
    for (vector<QueryGraph::Node>::const_iterator iter=subquery.nodes.begin(),limit=subquery.nodes.end();iter!=limit;++iter) {
      string s=buildFactsAttribute(id,"subject"),p=buildFactsAttribute(id,"predicate"),o=buildFactsAttribute(id,"object");
      if ((*iter).constSubject) {
	if (first) first=false; else cout << " and ";
	cout << s << "=" << (*iter).subject;
      } else if (representative[(*iter).subject]!=s) {
	if (first) first=false; else cout << " and ";
	cout << s << "=" << representative[(*iter).subject];
      }
      if ((*iter).constPredicate) {
	if (first) first=false; else cout << " and ";
	cout << p << "=" << (*iter).predicate;
      } else if (representative[(*iter).predicate]!=p) {
	if (first) first=false; else cout << " and ";
	cout << p << "=" << representative[(*iter).predicate];
      }
      if ((*iter).constObject) {
	if (first) first=false; else cout << " and ";
	cout << o << "=" << (*iter).object;
      } else if (representative[(*iter).object]!=o) {
	if (first) first=false; else cout << " and ";
	cout << o << "=" << representative[(*iter).object];
      }
      ++id;
    }
    f=id;
  }

 if (subquery.filters.size()) {
    map<unsigned,unsigned> varsfilters_map;
    set2map(varsfilters,varsfilters_map);
    unsigned id = 0;
    for(set<unsigned>::const_iterator iter=varsfilters.begin(),limit=varsfilters.end();iter!=limit;iter++) {
      cout << " and m" << id << ".id=" << representative[*iter];
      id++;
    }

    for(vector<QueryGraph::Filter>::iterator iter = subquery.filters.begin(), limit = subquery.filters.end() ; iter != limit ; iter++) {
     if((iter->type != QueryGraph::Filter::Builtin_bound) && (iter->arg1->type != QueryGraph::Filter::Builtin_bound)){
      cout << " and ";
      translateFilterPostgres(*iter,varsfilters_map);
     }
    }
  }
 
  cout << ")";
}

//---------------------------------------------------------------------------
// Name: translateUnionPostgres
// Authors: Giuseppe De Simone and Hancel Gonzalez
// Advisor: Maria Esther Vidal
// Description: For a given query translate the Union clause to 
//              the property table schema used by Postgres
//---------------------------------------------------------------------------
static void translateUnionPostgres(QueryGraph& query, vector<QueryGraph::SubQuery> unions, map<unsigned,unsigned>& projection, const string& schema, set<unsigned> unionvars) {
  unsigned id = 0;
  map<unsigned,unsigned> null;
  for (vector<QueryGraph::SubQuery>::const_iterator iter=unions.begin(), limit=unions.end() ; iter != limit ; ++iter) {
   if (id) 
     cout << "\n   UNION" << endl;
   translateSubQueryPostgres(query,*iter,projection,schema,unionvars,null);
   id++;
  }
}

//---------------------------------------------------------------------------
// Name: translateOptionalPostgres
// Authors: Giuseppe De Simone and Hancel Gonzalez
// Advisor: Maria Esther Vidal
// Description: This procedure translate the Optional clause for Postgres
//
//---------------------------------------------------------------------------
static void translateOptionalPostgres(QueryGraph& query, QueryGraph::SubQuery subquery, map<unsigned,unsigned>& projection, const string& schema, unsigned& f, unsigned& r, unsigned& fact, unsigned factbool) {
  unsigned fact_ini = fact;
  if (factbool && subquery.nodes.size()) {
    translateSubQueryOptionalPostgres(query,subquery,schema,f,r,projection,set<unsigned>());
    cout << "facts" << fact_ini;
  }
  
  set<unsigned> vars1;  
  vector<set<unsigned> > commons, vars;
  unsigned i=0;

  getVariables(subquery,vars1);
  vars.resize(subquery.optional.size());
  commons.resize(subquery.optional.size());
  for(vector<QueryGraph::SubQuery>::iterator iter=subquery.optional.begin(),limit=subquery.optional.end();iter!=limit;++iter) { 
    cout << "\n    LEFT OUTER JOIN" << endl;
    fact++;
    //Translate OPTIONAL clause inside OPTIONAL clause
    if ((*iter).nodes.size()) {
      getVariables(*iter,vars[i]);
      intersect(vars1,vars[i],commons[i]);
      translateSubQueryOptionalPostgres(query,*iter,schema,f,r,projection,commons[i]);
      onOptional(commons[i],fact_ini,fact);
      if ((*iter).optional.size())
        translateOptionalPostgres(query,*iter,projection,schema,f,r,fact,0);
    }
    else {
      //Translate UNION clause inside OPTIONAL clause 
      if ((*iter).unions.size() == 1){
        set<unsigned> vars2;
        getVariables((*iter).unions[0],vars2);
        intersect(vars1,vars2,commons[i]);
	cout << "(";
        translateUnionPostgres(query,(*iter).unions[0],projection,schema,commons[i]);
	cout << ")";
        onOptional(commons[i],fact_ini,fact);
      }
    }
    i++;
  }
}

//---------------------------------------------------------------------------
// Name: translateSubQueryPostgres
// Authors: Giuseppe De Simone and Hancel Gonzalez
// Advisor: Maria Esther Vidal
// Description: This procedure translate the Basic Group Graph Pattern for
//              queries with GJOIN clause to Postgres.
//---------------------------------------------------------------------------
static void translateSubQueryPostgres(QueryGraph::SubQuery subquery, pattern *structure, set<unsigned> common, unsigned tab, const string& schema) {

  // dictionary with the nodes elements
  map <unsigned, string> representative;

  {
    unsigned id=0;
    for (vector<QueryGraph::Node>::const_iterator iter=subquery.nodes.begin(),limit=subquery.nodes.end();iter!=limit;++iter) {
      if ((!(*iter).constSubject)&&(!representative.count((*iter).subject)))
				representative[(*iter).subject]=buildFactsAttribute(id,"subject");
      if ((!(*iter).constPredicate)&&(!representative.count((*iter).predicate)))
				representative[(*iter).predicate]=buildFactsAttribute(id,"predicate");
      if ((!(*iter).constObject)&&(!representative.count((*iter).object)))
				representative[(*iter).object]=buildFactsAttribute(id,"object");
      ++id;
    }
  }

	for (unsigned t = 0 ; t < tab ; t++)
		cout << "  ";

  cout << "(select ";
  {
    unsigned i=0;
    for (set<unsigned>::const_iterator iter=structure->vars->vars.begin(),limit=structure->vars->vars.end();iter!=limit;++iter) {      
			if (i) cout << ",";
			cout << representative[*iter] << " as " << "r" << *iter;
			i = 1; 
		}
		for (set<unsigned>::const_iterator iter=common.begin(),limit=common.end();iter!=limit;++iter) {      
			cout << "," << representative[*iter] << " as " << "q" << *iter;
		}
	}

  cout << endl;
	for (unsigned t = 0 ; t < tab ; t++)
		cout << "  ";
  cout << " from ";
  {
    unsigned id=0;
    for (vector<QueryGraph::Node>::const_iterator iter=subquery.nodes.begin(),limit=subquery.nodes.end();iter!=limit;++iter) {
      if (id) cout << ",";
      cout << schema << ".facts f" << id;
      ++id;
    }

  }
  set<unsigned> varsfilters;
  if (subquery.filters.size()) {
    cout << ",";
    getVariables(subquery.filters,varsfilters);
    unsigned id=0;
    for(set<unsigned>::const_iterator iter=varsfilters.begin(),limit=varsfilters.end();iter!=limit;iter++) {
      if(id) cout << ",";
      cout << schema << ".strings m" << id;
      id++;
    }
  }
  cout << endl;
 for (unsigned t = 0 ; t < tab ; t++)
		cout << "  ";

 if (!subquery.edges.empty() || 
     !subquery.filters.empty() || 
     (subquery.edges.empty() && ((*subquery.nodes.begin()).constSubject ||
                                 (*subquery.nodes.begin()).constObject))) 
	cout << " where ";
 {
    unsigned id=0; bool first=true;
    for (vector<QueryGraph::Node>::const_iterator iter=subquery.nodes.begin(),limit=subquery.nodes.end();iter!=limit;++iter) {
      string s=buildFactsAttribute(id,"subject"),p=buildFactsAttribute(id,"predicate"),o=buildFactsAttribute(id,"object");
      if ((*iter).constSubject) {
	if (first) first=false; else cout << " and ";
	cout << s << "=" << (*iter).subject;
      } else if (representative[(*iter).subject]!=s) {
	if (first) first=false; else cout << " and ";
	cout << s << "=" << representative[(*iter).subject];
      }
      if ((*iter).constPredicate) {
      } else if (representative[(*iter).predicate]!=p) {
	if (first) first=false; else cout << " and ";
	cout << p << "=" << representative[(*iter).predicate];
      }
      if ((*iter).constObject) {
	if (first) first=false; else cout << " and ";
	cout << o << "=" << (*iter).object;
      } else if (representative[(*iter).object]!=o) {
	if (first) first=false; else cout << " and ";
	cout << o << "=" << representative[(*iter).object];
      }
      ++id;
    }
  }
  //Join for get values for filters.
  if (subquery.filters.size()) {
    map<unsigned,unsigned> varsfilters_map;
    set2map(varsfilters,varsfilters_map);
    unsigned id = 0;
    for(set<unsigned>::const_iterator iter=varsfilters.begin(),limit=varsfilters.end();iter!=limit;iter++){
      cout << " and m" << id << ".id=" << representative[*iter];
      id++;
    }
    //Finally, translate filters.
    for(vector<QueryGraph::Filter>::iterator iter = subquery.filters.begin(), limit = subquery.filters.end() ; iter != limit ; iter++) {
      cout << " and "; 
      translateFilterPostgres(*iter,varsfilters_map);
    }
  }
 cout << ")";
}

//---------------------------------------------------------------------------
// Name: translateGJoinPostgres
// Authors: Giuseppe De Simone and Hancel Gonzalez
// Advisor: Maria Esther Vidal
// Description: This procedure translate the GJoin clause for Postgres
//
//---------------------------------------------------------------------------
static void translateGJoinPostgres(pattern *structure, QueryGraph::SubQuery query, set<unsigned> common, unsigned tab, const string& schema) {
	if(query.gjoins.empty()) {
		translateSubQueryPostgres(query,structure,common,tab,schema);
	}
	else {
		for (unsigned t = 0 ; t < tab ; t++)
			cout << "  ";
		cout << "(select ";
	  {
  	  unsigned i=0;
	    for (set<unsigned>::const_iterator iter=structure->vars->vars.begin(),limit=structure->vars->vars.end();iter!=limit;++iter) {      
				if (i) cout << ",";

				if (structure->arg1->vars->vars.count(*iter))
					cout << "p1";
				else
					cout << "p2";

				cout << ".r" << *iter << " as " << "r" << *iter;
				i = 1; 
			}
			for (set<unsigned>::const_iterator iter=common.begin(),limit=common.end();iter!=limit;++iter) {      
				cout << ",";
				if (structure->arg1->vars->vars.count(*iter))
					cout << "p1";
				else
					cout << "p2";
				cout << ".r" << *iter << " as " << "q" << *iter;
			}
		}
		cout << endl;
		for (unsigned t = 0 ; t < tab ; t++)
			cout << "  ";
		cout << " from " << endl;
		translateGJoinPostgres(structure->arg1,query.gjoins[0][0],structure->vars->commons,tab+1,schema);
		cout << " p1," << endl;
		translateGJoinPostgres(structure->arg2,query.gjoins[0][1],structure->vars->commons,tab+1,schema);
		cout << " p2 " << endl;
		for (unsigned t = 0 ; t < tab ; t++)
			cout << "  ";
		cout << " where ";
		unsigned i = 0;
		for (set<unsigned>::const_iterator iter=structure->vars->commons.begin(),limit=structure->vars->commons.end();iter!=limit;++iter) {
			if (i) cout << " and ";
			cout << "p1.q" << *iter << " = " << "p2.q" << *iter;
			i = 1;
		}
		cout << ")";
	}
}

//---------------------------------------------------------------------------
// Name: translatePostgres
// Authors: Giuseppe De Simone and Hancel Gonzalez
// Advisor: Maria Esther Vidal
// Description: For a given QueryGraph and a SubQuery of itself
//              This function catch what operators are in it(optional,union 
//              or neither one of them), then calls a procedure that it's 
//              going to continue whith the translation.

//---------------------------------------------------------------------------
static void translatePostgres(QueryGraph& query, QueryGraph::SubQuery subquery, const string& schema) {
  map<unsigned,unsigned> projection,null;
  // No OPTIONAL, No UNION
  if (!subquery.optional.size() && !subquery.unions.size()) {
 		// No gjoin
		if (subquery.gjoins.empty())
	    translateSubQueryPostgres(query, subquery, projection, schema, set<unsigned>(), null);
		// Gjoin clause
		else {
			pattern *structure;
			structure = new pattern;
			query2structure(query.getQuery(),structure);	
			translateGJoinPostgres(structure,subquery,set<unsigned>(),1,schema);
		} 
  }
  // Only UNION clause
  else if (!subquery.optional.size() && subquery.unions.size() == 1) {
    translateUnionPostgres(query,subquery.unions[0],projection,schema,set<unsigned>());
  }
  // Only OPTIONAL clause
  else if (subquery.optional.size() && !subquery.unions.size()) {
    unsigned f = 0, r = 0, fact = 0;
    translateOptionalPostgres(query,subquery,projection,schema,f,r,fact,1); 
  }
}
//---------------------------------------------------------------------------
// Name: dumpPostgres
// Modified by: Giuseppe De Simone and Hancel Gonzalez
// Advisor: Maria Esther Vidal
// Description: This is an modification for the original function.
//              For a given QueryGraph returns a equivalent query
//              to be run on the schema for Postgres
//             
//--------------------------------------------------------------------------
static void dumpPostgres(QueryGraph& query,const string& schema) {
  // Dump a postgres query
  set<unsigned> projection;
  
  cout << "select ";
  if (query.getDuplicateHandling() == 3)
    cout << "distinct ";
  {
    unsigned id=0;
    for (QueryGraph::projection_iterator iter=query.projectionBegin(),limit=query.projectionEnd();iter!=limit;++iter) {
      if (id) cout << ",";
      cout << "s" << id << ".value";
      id++;
      projection.insert(*iter);
    }
  }
  cout << endl;
  cout << "from (" << endl;

  QueryGraph::SubQuery subquery = query.getQuery(); 

  translatePostgres(query,subquery,schema);

  cout << endl << ") facts";

  if(subquery.optional.size() || subquery.unions.size()){
    set<unsigned> vars_uoggp;
    {
      set<unsigned> vars_bggp;
      getVariables(subquery,vars_bggp);
      if(subquery.optional.size())
        getOptionalsVariables(subquery.optional,vars_uoggp);
      else
        getVariables(subquery.unions[0],vars_uoggp);
      difference(vars_uoggp,vars_bggp);
      {
        unsigned id=projection.size() - 1;
        for (set<unsigned>::iterator iter=projection.end(),limit=projection.begin();iter!=limit;--iter) {
          if(vars_uoggp.count(*iter - 1))
            cout << " LEFT OUTER JOIN " << schema << ".strings s" << id << " ON(" << "s" << id << ".id=facts.r" << id << ")";
          id--;
       }
       id=projection.size() - 1;
       for (set<unsigned>::iterator iter=projection.end(),limit=projection.begin();iter!=limit;--iter) {
          if(!vars_uoggp.count(*iter - 1))
            cout << "," << schema <<".strings s" << id;
          id--;
        }
      }
    }
    cout << endl;
    {
      unsigned id=projection.size() - 1, y = 0;
      for (set<unsigned>::iterator iter=projection.end(),limit=projection.begin();iter!=limit;--iter) {
        if(!vars_uoggp.count(*iter - 1)) {
          if(!y) cout << "where ";
          if (y) cout << " and ";
          cout << "s" << id << ".id=facts.r" << id;
          y = 1;
        }
        id--;
      }
    }
  }
  else {
    unsigned id=0;
    for (QueryGraph::projection_iterator iter=query.projectionBegin(),limit=query.projectionEnd();iter!=limit;++iter) {
      cout << "," << schema <<".strings s" << id;
      id++;
    }
    cout << endl;
    cout << "where ";
    {
      unsigned id=0;
      for (QueryGraph::projection_iterator iter=query.projectionBegin(),limit=query.projectionEnd();iter!=limit;++iter) {
        if (id) cout << " and ";
        cout << "s" << id << ".id=facts.r" << id;
        id++;
      }
    }
  }
 cout << endl;

 {
 if (query.getLimit() != ~0u) {
   cout << endl;
   cout << "limit " << query.getLimit();
 }

  unsigned id=0, o=1;
  for (QueryGraph::order_iterator iter=query.orderBegin(),limit=query.orderEnd();iter!=limit;++iter) {
    if (o) { cout << "order by "; o = 0; }
    if (id) cout << ",";
    cout << "s" << (*iter).id << ".value";
    id++;
  }
 }

 cout << ";" << endl;
}
//---------------------------------------------------------------------------
// Name: translateFilterMonetDB
// Authors: Giuseppe De Simone and Hancel Gonzalez
// Advisor: Maria Esther Vidal
// Description: Make the translation for all the filters that  has to 
//              be translate to the property table schema of MonetDB 
//--------------------------------------------------------------------------- 
static void translateFilterMonetDB(QueryGraph::Filter filter, map<unsigned,string> representative) {
  map<QueryGraph::Filter::Type,string> binaryOperator; 
  binaryOperator[QueryGraph::Filter::Or]             = "or";
  binaryOperator[QueryGraph::Filter::And]            = "and";
  binaryOperator[QueryGraph::Filter::Equal]          = "=";
  binaryOperator[QueryGraph::Filter::NotEqual]       = "<>";
  binaryOperator[QueryGraph::Filter::Less]           = "<";
  binaryOperator[QueryGraph::Filter::LessOrEqual]    = "<=";
  binaryOperator[QueryGraph::Filter::Greater]        = ">";
  binaryOperator[QueryGraph::Filter::GreaterOrEqual] = ">=";
  binaryOperator[QueryGraph::Filter::Plus]           = "+";
  binaryOperator[QueryGraph::Filter::Minus]          = "-";
  binaryOperator[QueryGraph::Filter::Mul]            = "*";
  binaryOperator[QueryGraph::Filter::Div]            = "/";
  binaryOperator[QueryGraph::Filter::Builtin_regex]  = "like";

  map<QueryGraph::Filter::Type,string> unaryOperator;
  unaryOperator[QueryGraph::Filter::Not]        = "not";
  unaryOperator[QueryGraph::Filter::UnaryPlus]  = "+";
  unaryOperator[QueryGraph::Filter::UnaryMinus] = "-"; 
  
  if (filter.type == QueryGraph::Filter::Variable)  
    cout << "cast((select value from strings where id = " << representative[filter.id] << ") as integer)";
  else if (filter.type == QueryGraph::Filter::Literal)
    cout << filter.value;
  else if (filter.type == QueryGraph::Filter::IRI)
    cout << "\'" << filter.value << "\'";
		//cout << filter.id << endl;
  else if (unaryOperator.count(filter.type)) {
   if(filter.arg1->type != QueryGraph::Filter::Builtin_bound) {
    cout << unaryOperator[filter.type] << "(";
    translateFilterMonetDB(*filter.arg1,representative);
    cout << ")";
   }
  }
  else if (binaryOperator.count(filter.type)){
    cout << "(";
    translateFilterMonetDB(*filter.arg1,representative);
    cout << " " << binaryOperator[filter.type] << " ";
    if(filter.type == QueryGraph::Filter::Builtin_regex) {
      cout << "'%";
      translateFilterMonetDB(*filter.arg2,representative);
      cout << "%'";
    }
    else
      translateFilterMonetDB(*filter.arg2,representative);
    cout << ")";
  }
}
//---------------------------------------------------------------------------
// Name: translateSubQueryMonetDB
// Modified by: Giuseppe De Simone and Hancel Gonzalez
// Advisor: Maria Esther Vidal
// Description: For a given QueryGraph with no optionals or unions
//              make the translation to the property table schema of
//              MonetDB
//---------------------------------------------------------------------------
static void translateSubQueryMonetDB(QueryGraph& query, QueryGraph::SubQuery subquery, map<unsigned,unsigned>& projection, set<unsigned> unionvars, map<unsigned,unsigned>& null) {

  // dictionary with the nodes elements
  map <unsigned, string> representative;

  {
    unsigned id=0;
    for (vector<QueryGraph::Node>::const_iterator iter=subquery.nodes.begin(),limit=subquery.nodes.end();iter!=limit;++iter) {
      if ((!(*iter).constSubject)&&(!representative.count((*iter).subject)))
	representative[(*iter).subject]=buildFactsAttribute(id,"subject");
      if ((!(*iter).constPredicate)&&(!representative.count((*iter).predicate)))
	representative[(*iter).predicate]=buildFactsAttribute(id,"predicate");
      if ((!(*iter).constObject)&&(!representative.count((*iter).object)))
	representative[(*iter).object]=buildFactsAttribute(id,"object");
      ++id;
    }
  }
  cout << "   (select ";
  {
    unsigned id=0, i=0;
    for (QueryGraph::projection_iterator iter=query.projectionBegin(),limit=query.projectionEnd();iter!=limit;++iter) {      
      if (i) cout << ",";

      if(!projection.count(*iter)) {
	if(representative.count(*iter)) {
	  cout << representative[*iter];
	  projection[*iter]=1;
	  null[*iter] = 1;
	}
	else if (!representative.count(*iter))// && null.count(*iter))
	  cout << " NULL";
	cout << " as r" << id;
	i=1;
      
      } else {
	if (representative.count(*iter)) {
	  cout << representative[*iter] << " as r" << id;
	  i=1;
	} else if (null.count(*iter)) {
	  cout << " NULL as r" << id;
	  i = 1;
	}
      }
      id++;
    }
  }

  cout << endl << "    from ";
  {
    unsigned id=0;
    for (vector<QueryGraph::Node>::const_iterator iter=subquery.nodes.begin(),limit=subquery.nodes.end();iter!=limit;++iter) {
      if (id) cout << ",";
      if ((*iter).constPredicate)
	cout << "p" << (*iter).predicate << " f" << id; else
	cout << "allproperties f" << id;
      ++id;
    }
  }
  cout << endl;
  cout << "    where ";
  {
    unsigned id=0; bool first=true;
    for (vector<QueryGraph::Node>::const_iterator iter=subquery.nodes.begin(),limit=subquery.nodes.end();iter!=limit;++iter) {
      string s=buildFactsAttribute(id,"subject"),p=buildFactsAttribute(id,"predicate"),o=buildFactsAttribute(id,"object");
      if ((*iter).constSubject) {
	if (first) first=false; else cout << " and ";
	cout << s << "=" << (*iter).subject;
      } else if (representative[(*iter).subject]!=s) {
	if (first) first=false; else cout << " and ";
	cout << s << "=" << representative[(*iter).subject];
      }
      if ((*iter).constPredicate) {
      } else if (representative[(*iter).predicate]!=p) {
	if (first) first=false; else cout << " and ";
	cout << p << "=" << representative[(*iter).predicate];
      }
      if ((*iter).constObject) {
	if (first) first=false; else cout << " and ";
	cout << o << "=" << (*iter).object;
      } else if (representative[(*iter).object]!=o) {
	if (first) first=false; else cout << " and ";
	cout << o << "=" << representative[(*iter).object];
      }
      ++id;
    }
  }   
  if (subquery.filters.size())
    //Finally, translate filters.
    for(vector<QueryGraph::Filter>::iterator iter = subquery.filters.begin(), limit = subquery.filters.end() ; iter != limit ; iter++) {
      cout << " and "; 
      translateFilterMonetDB(*iter,representative);
    }
  
  cout << ")";
}
//---------------------------------------------------------------------------
// Name: translateSubQueryOptionalMonetDB
// Authors: Giuseppe De Simone and Hancel Gonzalez
// Advisor: Maria Esther Vidal
// Description: Build the body of the optional translation 
//---------------------------------------------------------------------------
static void translateSubQueryOptionalMonetDB(QueryGraph& query, QueryGraph::SubQuery subquery, unsigned& f, unsigned& r, map<unsigned,unsigned>& projection,set<unsigned> optionalvars) {

  map<unsigned,string> representative;
  set<unsigned> vars1;
  vector<set<unsigned> > vars2;
  set<unsigned> common;
  {
    unsigned id=f;
    //Create dictionary with elements of nodes.
    for (vector<QueryGraph::Node>::const_iterator iter=subquery.nodes.begin(),limit=subquery.nodes.end();iter!=limit;++iter) {
      if ((!(*iter).constSubject)&&(!representative.count((*iter).subject)))
	representative[(*iter).subject]=buildFactsAttribute(id,"subject");
      if ((!(*iter).constPredicate)&&(!representative.count((*iter).predicate)))
	representative[(*iter).predicate]=buildFactsAttribute(id,"predicate");
      if ((!(*iter).constObject)&&(!representative.count((*iter).object)))
	representative[(*iter).object]=buildFactsAttribute(id,"object");
      ++id;
    }
  }
  getVariables(subquery,vars1);
  //OPTIONAL inside this OPTIONAL clause? Get variables for JOIN
  if (subquery.optional.size()) {
    unsigned i=0;
    vars2.resize(subquery.optional.size());
    common.clear();
    for(vector<QueryGraph::SubQuery>::iterator iter=subquery.optional.begin(),limit=subquery.optional.end();iter!=limit;++iter) {
      getVariables(*iter,vars2[i]);
      intersect(vars1,vars2[i],common);
      i++;
      if ((*iter).unions.size() == 1) {
        getVariables((*iter).unions[0],vars2[0]);
        intersect(vars1,vars2[0],common);
      }
    }
  }

  cout << "   (select ";
  {
    unsigned id=r, tmp=0;
    for (QueryGraph::projection_iterator iter=query.projectionBegin(),limit=query.projectionEnd();iter!=limit;++iter) {
      //Don't repeat variables in SELECT clause
      if (representative.count(*iter) && !projection.count(*iter)) {
        projection[*iter] = 1;
        if (tmp) cout << ",";
        cout << representative[*iter] << " as r" << id;
        id++;
        tmp=1;
      }
    }
    r = id;
  }
  {
    unsigned id = 0;
    //Select variables from JOIN of OPTIONAL
    if (optionalvars.size())
      for(set<unsigned>::iterator iter=optionalvars.begin(),limit=optionalvars.end();iter!=limit;++iter){ 
	cout << "," << representative[*iter] << " as q" << id;
	id++;
      }

    id = 0;
    //Select variables to JOIN of OPTIONAL
    if (subquery.optional.size() || subquery.unions[0].size() == 1)
      for(set<unsigned>::iterator iter=common.begin(),limit=common.end();iter!=limit;++iter) {
	cout << "," << representative[*iter] << " as p" << id;
	id++;
      }
  }

  cout << endl << "    from ";
  {
    unsigned id=f;
    unsigned i=0;
    for (vector<QueryGraph::Node>::const_iterator iter=subquery.nodes.begin(),limit=subquery.nodes.end();iter!=limit;++iter) {
      if (i) cout << ",";
      if ((*iter).constPredicate)
	cout << "p" << (*iter).predicate << " f" << id; else
	cout << "allproperties f" << id;
      ++id;
      ++i;
    }
    
  }
  if (subquery.nodes.size() > 1 || subquery.filters.size() != 0) {
    cout << endl << "    where ";
  }
  {
    unsigned id=f; bool first=true;
    for (vector<QueryGraph::Node>::const_iterator iter=subquery.nodes.begin(),limit=subquery.nodes.end();iter!=limit;++iter) {
      string s=buildFactsAttribute(id,"subject"),p=buildFactsAttribute(id,"predicate"),o=buildFactsAttribute(id,"object");
      if ((*iter).constSubject) {
	if (first) first=false; else cout << " and ";
	cout << s << "=" << (*iter).subject;
      } else if (representative[(*iter).subject]!=s) {
	if (first) first=false; else cout << " and ";
	cout << s << "=" << representative[(*iter).subject];
      }
      if ((*iter).constPredicate) {
      } else if (representative[(*iter).predicate]!=p) {
	if (first) first=false; else cout << " and ";
	cout << p << "=" << representative[(*iter).predicate];
      }
      if ((*iter).constObject) {
	if (first) first=false; else cout << " and ";
	cout << o << "=" << (*iter).object;
      } else if (representative[(*iter).object]!=o) {
	if (first) first=false; else cout << " and ";
	cout << o << "=" << representative[(*iter).object];
      }
      ++id;
    }
    f=id;
  }
  if (subquery.filters.size()) {  
    //Finally, translate filters.
    for(vector<QueryGraph::Filter>::iterator iter = subquery.filters.begin(), limit = subquery.filters.end() ; iter != limit ; iter++) {
     if((iter->type != QueryGraph::Filter::Builtin_bound) && (iter->arg1->type != QueryGraph::Filter::Builtin_bound)){
      cout << " and "; 
      translateFilterMonetDB(*iter,representative);
     }
    }
  }
  cout << ")";
}
//---------------------------------------------------------------------------
// Name: translateUnionMonetDB
// Authors: Giuseppe De Simone and Hancel Gonzalez
// Advisor: Maria Esther Vidal
// Description: For a given query translate the Union clause to 
//              the property table schema used by MonetDB 
//---------------------------------------------------------------------------
static void translateUnionMonetDB(QueryGraph& query, vector<QueryGraph::SubQuery>  unions, map<unsigned,unsigned>& projection, set<unsigned> unionvars) {

  unsigned i = 0;
  map<unsigned,unsigned> null;
  for (vector<QueryGraph::SubQuery>::const_iterator iter=unions.begin(), limit=unions.end() ; iter != limit ; ++iter) {
   if (i) 
     cout << "\n   UNION" << endl;
   translateSubQueryMonetDB(query,*iter,projection,unionvars,null);
   i++;
  }
}

//---------------------------------------------------------------------------
// Name: translateOptionalMonetDB
// Authors: Giuseppe De Simone and Hancel Gonzalez
// Advisor: Maria Esther Vidal
// Description: This procedure translate the Optional clause for MonetDB
//
//--------------------------------------------------------------------------
static void translateOptionalMonetDB(QueryGraph& query, QueryGraph::SubQuery subquery,map<unsigned,unsigned>& projection, unsigned& f, unsigned& r, unsigned& fact, unsigned factbool) {
  unsigned fact_ini= fact;
  if (factbool && subquery.nodes.size()) {
    translateSubQueryOptionalMonetDB(query, subquery, f,r, projection, set<unsigned>());
    cout << " facts" << fact_ini;
  }

  set<unsigned> vars1;  
  vector<set<unsigned> > commons, vars;
  unsigned i=0;

  getVariables(subquery,vars1);
  vars.resize(subquery.optional.size());
  commons.resize(subquery.optional.size());
   for(vector<QueryGraph::SubQuery>::iterator iter=subquery.optional.begin(),limit=subquery.optional.end();iter!=limit;++iter) { 
    cout << "\n    LEFT OUTER JOIN" << endl;
    fact++;
    
    //Translate OPTIONAL clause inside OPTIONAL clause
    if ((*iter).nodes.size()) {

      getVariables(*iter,vars[i]);
      intersect(vars1,vars[i],commons[i]);
      translateSubQueryOptionalMonetDB(query,*iter,f,r,projection,commons[i]);
      onOptional(commons[i],fact_ini,fact);
      if ((*iter).optional.size())
        translateOptionalMonetDB(query,*iter,projection,f,r,fact,0);
    }
    else {
      //Translate UNION clause inside OPTIONAL clause 
      if ((*iter).unions.size() == 1){
        set<unsigned> vars2;
        getVariables((*iter).unions[0],vars2);
        intersect(vars1,vars2,commons[i]);
	cout << "(";
        translateUnionMonetDB(query,(*iter).unions[0],projection,commons[i]);
	cout << ")";
        onOptional(commons[i],fact_ini,fact);
      }
    }
    i++;
   }

   cout  << endl;
}

//---------------------------------------------------------------------------
// Name: translateSubQueryMonetDB
// Authors: Giuseppe De Simone and Hancel Gonzalez
// Advisor: Maria Esther Vidal
// Description: This procedure translate the Basic Group Graph Pattern for
//              queries with GJOIN clause to MonetDB.
//---------------------------------------------------------------------------
static void translateSubQueryMonetDB(QueryGraph::SubQuery subquery, pattern *structure, set<unsigned> common, unsigned tab) {

  // dictionary with the nodes elements
  map <unsigned, string> representative;

  {
    unsigned id=0;
    for (vector<QueryGraph::Node>::const_iterator iter=subquery.nodes.begin(),limit=subquery.nodes.end();iter!=limit;++iter) {
      if ((!(*iter).constSubject)&&(!representative.count((*iter).subject)))
				representative[(*iter).subject]=buildFactsAttribute(id,"subject");
      if ((!(*iter).constPredicate)&&(!representative.count((*iter).predicate)))
				representative[(*iter).predicate]=buildFactsAttribute(id,"predicate");
      if ((!(*iter).constObject)&&(!representative.count((*iter).object)))
				representative[(*iter).object]=buildFactsAttribute(id,"object");
      ++id;
    }
  }

	for (unsigned t = 0 ; t < tab ; t++)
		cout << "  ";

  cout << "(select ";
  {
    unsigned i=0;
    for (set<unsigned>::const_iterator iter=structure->vars->vars.begin(),limit=structure->vars->vars.end();iter!=limit;++iter) {      
			if (i) cout << ",";
			cout << representative[*iter] << " as " << "r" << *iter;
			i = 1; 
		}
		for (set<unsigned>::const_iterator iter=common.begin(),limit=common.end();iter!=limit;++iter) {      
			cout << "," << representative[*iter] << " as " << "q" << *iter;
		}
	}

  cout << endl;
	for (unsigned t = 0 ; t < tab ; t++)
		cout << "  ";
  cout << " from ";

  {
    unsigned id=0;
    for (vector<QueryGraph::Node>::const_iterator iter=subquery.nodes.begin(),limit=subquery.nodes.end();iter!=limit;++iter) {
      if (id) cout << ",";
      if ((*iter).constPredicate)
	cout << "p" << (*iter).predicate << " f" << id; else
	cout << "allproperties f" << id;
      ++id;
    }
  }
  cout << endl;
 for (unsigned t = 0 ; t < tab ; t++)
		cout << "  ";

 if (!subquery.edges.empty() || 
     !subquery.filters.empty() || 
     (subquery.edges.empty() && ((*subquery.nodes.begin()).constSubject ||
                                 (*subquery.nodes.begin()).constObject))) 
	cout << " where ";
 {
    unsigned id=0; bool first=true;
    for (vector<QueryGraph::Node>::const_iterator iter=subquery.nodes.begin(),limit=subquery.nodes.end();iter!=limit;++iter) {
      string s=buildFactsAttribute(id,"subject"),p=buildFactsAttribute(id,"predicate"),o=buildFactsAttribute(id,"object");
      if ((*iter).constSubject) {
	if (first) first=false; else cout << " and ";
	cout << s << "=" << (*iter).subject;
      } else if (representative[(*iter).subject]!=s) {
	if (first) first=false; else cout << " and ";
	cout << s << "=" << representative[(*iter).subject];
      }
      if ((*iter).constPredicate) {
      } else if (representative[(*iter).predicate]!=p) {
	if (first) first=false; else cout << " and ";
	cout << p << "=" << representative[(*iter).predicate];
      }
      if ((*iter).constObject) {
	if (first) first=false; else cout << " and ";
	cout << o << "=" << (*iter).object;
      } else if (representative[(*iter).object]!=o) {
	if (first) first=false; else cout << " and ";
	cout << o << "=" << representative[(*iter).object];
      }
      ++id;
    }
  }   
  if (subquery.filters.size())
    //Finally, translate filters.
		if (!subquery.edges.empty()) cout << " and ";
		unsigned i = 0;
    for(vector<QueryGraph::Filter>::iterator iter = subquery.filters.begin(), limit = subquery.filters.end() ; iter != limit ; iter++) {
      if (i) cout << " and "; 
      translateFilterMonetDB(*iter,representative);
			i++;
    }
  
 cout << ")";
}

//---------------------------------------------------------------------------
// Name: translateGJoinMonetDB
// Authors: Giuseppe De Simone and Hancel Gonzalez
// Advisor: Maria Esther Vidal
// Description: This procedure translate the GJoin clause for MonetDB
//
//---------------------------------------------------------------------------
static void translateGJoinMonetDB(pattern *structure, QueryGraph::SubQuery query, set<unsigned> common, unsigned tab) {
	if(query.gjoins.empty()) {
		translateSubQueryMonetDB(query,structure,common,tab);
	}
	else {
		for (unsigned t = 0 ; t < tab ; t++)
			cout << "  ";
		cout << "(select ";
	  {
  	  unsigned i=0;
	    for (set<unsigned>::const_iterator iter=structure->vars->vars.begin(),limit=structure->vars->vars.end();iter!=limit;++iter) {      
				if (i) cout << ",";

				if (structure->arg1->vars->vars.count(*iter))
					cout << "p1";
				else
					cout << "p2";

				cout << ".r" << *iter << " as " << "r" << *iter;
				i = 1; 
			}
			for (set<unsigned>::const_iterator iter=common.begin(),limit=common.end();iter!=limit;++iter) {      
				cout << ",";
				if (structure->arg1->vars->vars.count(*iter))
					cout << "p1";
				else
					cout << "p2";
				cout << ".r" << *iter << " as " << "q" << *iter;
			}
		}
		cout << endl;
		for (unsigned t = 0 ; t < tab ; t++)
			cout << "  ";
		cout << " from " << endl;
		translateGJoinMonetDB(structure->arg1,query.gjoins[0][0],structure->vars->commons,tab+1);
		cout << " p1," << endl;
		translateGJoinMonetDB(structure->arg2,query.gjoins[0][1],structure->vars->commons,tab+1);
		cout << " p2 " << endl;
		for (unsigned t = 0 ; t < tab ; t++)
			cout << "  ";
		cout << " where ";
		unsigned i = 0;
		for (set<unsigned>::const_iterator iter=structure->vars->commons.begin(),limit=structure->vars->commons.end();iter!=limit;++iter) {
			if (i) cout << " and ";
			cout << "p1.q" << *iter << " = " << "p2.q" << *iter;
			i = 1;
		}
		cout << ")";
	}
}

//---------------------------------------------------------------------------
// Name: translateMonetDB
// Authors: Giuseppe De Simone and Hancel Gonzalez
// Advisor: Maria Esther Vidal
// Description: For a given QueryGraph and a SubQuery of itself
//              This function catch what operators are in it(optional,union 
//              or neither one of them), then calls a procedure that it's 
//              going to continue whith the translation
//----------------------------------------------------------------------------
static void translateMonetDB(QueryGraph& query, QueryGraph::SubQuery subquery){
  map <unsigned, unsigned> projection, null;
  
  // No optional, No Union
  if(!subquery.optional.size() && !subquery.unions.size()) {
		// No gjoin
		if (subquery.gjoins.empty())
	    translateSubQueryMonetDB(query, subquery, projection, set<unsigned>(), null);
		// Gjoin clause
		else {
			pattern *structure;
			structure = new pattern;
			query2structure(query.getQuery(),structure);	
			translateGJoinMonetDB(structure,query.getQuery(),set<unsigned>(),1);
		}
  }
  // Union clause 
  else if(!subquery.optional.size() && subquery.unions.size() == 1) {
    translateUnionMonetDB(query, subquery.unions[0],projection, set<unsigned>());
  }

  // Optional clause
  else if(subquery.optional.size()  && !subquery.unions.size()) {
    unsigned  f = 0, r = 0, fact = 0;

    cout << "  select ";
    {
      unsigned id=0;
      for (QueryGraph::projection_iterator iter=query.projectionBegin(),limit=query.projectionEnd();iter!=limit;++iter) {
	if (id) cout << ",";
	cout << "r" << id;
	id++;
      }
    }
    cout << endl;
    cout << "  from (" << endl;
    translateOptionalMonetDB(query,subquery, projection, f,r,fact,1);
    cout << "  )";
  }
}

//---------------------------------------------------------------------------
// Name: dumpMonetDB
// Modified by: Giuseppe De Simone and Hancel Gonzalez
// Advisor: Maria Esther Vidal
// Description: This is an modification for the original function.
//              For a given QueryGraph returns a equivalent query
//              to be run on the property table schema of MonetDB
//             
//--------------------------------------------------------------------------
static void dumpMonetDB(QueryGraph& query) {
  // Dump a monet query
  set<unsigned> projection;

  cout << "select ";
  if(query.getDuplicateHandling()==3) 
    cout << "distinct ";
  {
    unsigned id=0;
    for (QueryGraph::projection_iterator iter=query.projectionBegin(),limit=query.projectionEnd();iter!=limit;++iter) {
      if (id) cout << ",";
      cout << "s" << id << ".value";
      id++;
      projection.insert(*iter);
    }
  }
  cout << endl;
  cout << "from (" << endl;

  QueryGraph::SubQuery subquery = query.getQuery();
  
  translateMonetDB(query,subquery);

  cout << endl << ")facts";

  if(subquery.optional.size() || subquery.unions.size()){
    set<unsigned> vars_uoggp, vars_bggp;
    getVariables(subquery,vars_bggp);
    if(subquery.optional.size())
      getOptionalsVariables(subquery.optional,vars_uoggp);
    else if (subquery.unions[0].size())
      getVariables(subquery.unions[0],vars_uoggp);
    difference(vars_uoggp,vars_bggp);
    {
      unsigned id=projection.size() - 1;
      for (set<unsigned>::iterator iter=projection.end(),limit=projection.begin();iter!=limit;--iter) {
        if(vars_uoggp.count(*iter - 1)) 
          cout << " LEFT OUTER JOIN strings s" << id << " ON(" << "s" << id << ".id=facts.r" << id << ")";

        id--;
      }
      id=projection.size() - 1;
      for (set<unsigned>::iterator iter=projection.end(),limit=projection.begin();iter!=limit;--iter) {
        if(!vars_uoggp.count(*iter - 1)) 
          cout << ",strings s" << id;
        id--;
      }
    }
    cout << endl;
    {
      unsigned id=projection.size() - 1, y=0;
      for (set<unsigned>::iterator iter=projection.end(),limit=projection.begin();iter!=limit;--iter) {
        if(!vars_uoggp.count(*iter - 1)){
          if(!y) cout << "where ";
          if (y) cout << " and ";
          cout << "s" << id << ".id=facts.r" << id;
          y=1;
        }
        id--;
      }
    }    
  }
  else {
    unsigned id=0;
    for (QueryGraph::projection_iterator iter=query.projectionBegin(),limit=query.projectionEnd();iter!=limit;++iter) {
      cout << ", strings s" << id;
      id++;
    }
    cout << endl;
    cout << "where ";
    {
      unsigned id=0;
      for (QueryGraph::projection_iterator iter=query.projectionBegin(),limit=query.projectionEnd();iter!=limit;++iter) {
        if (id) cout << " and ";
        cout << "s" << id << ".id=facts.r" << id;
        id++;
      }
    }      
  }

  if (query.getLimit() != ~0u)  
    cout << endl << "limit " << query.getLimit();
  {                                                                                               
    unsigned id=0, o=1;   
    for (QueryGraph::order_iterator iter=query.orderBegin(),limit=query.orderEnd();iter!=limit;++iter) { 
      if (o) { cout << endl << "order by "; o = 0; }                                                                                                                          
      if (id) cout << ",";                                                                                                                   
      cout << "s" << (*iter).id << ".value";
      id++;                                                                                                            
    }
  }  
  cout << ";" << endl;
}
//---------------------------------------------------------------------------
int main(int argc,char* argv[]) {
  // Check the arguments
  if (argc<3) {
    cout << "usage: " << argv[0] << " <database> <type> [sparqlfile]" << endl;
    return 1;
  }
  bool monetDB=false;
  if (string(argv[2])=="postgres")
    monetDB=false; 
  else if (string(argv[2])=="monetdb")
      monetDB=true; 
  else {
      cout << "unknown method " << argv[2] << endl;
      return 1;
  }

  // Open the database
  Database db;
  if (!db.open(argv[1])) {
    cout << "unable to open database " << argv[1] << endl;
    return 1;
  }

  // Retrieve the query
  string query;
  if (argc>3) {
    ifstream in(argv[3]);
    if (!in.is_open()) {
      cout << "unable to open " << argv[3] << endl;
      return 1;
    }
    query=readInput(in);
  } else {
    query=readInput(cin);
  }

  // Parse it
  QueryGraph queryGraph;
  {
    // Parse the query
    SPARQLLexer lexer(query);
    SPARQLParser parser(lexer);
    try {
      parser.parse();
    } catch (const SPARQLParser::ParserException& e) {
      cout << "parse error: " << e.message << endl;
      return 1;
    }

    // And perform the semantic anaylsis
    SemanticAnalysis semana(db);
    semana.transform(parser,queryGraph);
    if (queryGraph.knownEmpty()) {
      cout << "<empty result>" << endl;
      return 1;
    }
}

  // And dump it
  if (monetDB)
    dumpMonetDB(queryGraph); 
  else
    dumpPostgres(queryGraph,databaseName(argv[1]));
}

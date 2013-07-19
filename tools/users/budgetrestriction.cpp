#include <string>
#include "rts/dijkstra/DijkstraEngine.hpp"
#include "rts/dijkstra/ApproxPathEngine.hpp"
#include "rts/database/Database.hpp"
#include "rts/segment/DictionarySegment.hpp"
#include "infra/osdep/Timestamp.hpp"
#include "rts/segment/FactsSegment.hpp"
#include "rts/operator/AggregatedIndexScan.hpp"
#include "rts/operator/IndexScan.hpp"
#include "rts/operator/MergeJoin.hpp"
#include "rts/runtime/Runtime.hpp"
#include "rts/segment/AggregatedFactsSegment.hpp"
#include "rts/segment/FullyAggregatedFactsSegment.hpp"
#include <iostream>
#include <fstream>
#include <vector>
#include <list>
#include <set>
#include <algorithm>
#include <cassert>
#include <cmath>
#include <queue>
#include <set>
#include <map>
#include <limits>

using namespace std;

template <typename T1, typename T2>
struct PathLengthComparator {
public:

    bool operator()(const pair<T1, T2>& p1, const pair<T1, T2>& p2) const {

        if (p1.first == p2.first)
            return (p1.second) < (p2.second);
        return p1.first < p2.first;
    };

};
typedef ApproxPathEngine::set_path set_path;

static string lookupLiteral(Database& db,unsigned id)
   // Lookup a literal value
{
   const char* start=0,*end=start; Type::ID type; unsigned subType;
   db.getDictionary().lookupById(id,start,end,type,subType);
   return string(start,end);
}

static vector<unsigned> pathLengthDistribution(set_path paths) {
    vector<unsigned> res;
    if (paths.size() == 0)
        return res;
    unsigned shortestpath = (*paths.begin()).first;
    unsigned longestpath = (*paths.rbegin()).first;
    cout << "Total num of paths = " << paths.size() << endl;
    for (unsigned i = 0; i <= longestpath - shortestpath; i++)
        res.push_back(0);
    for (set_path::iterator it = paths.begin(); it != paths.end(); it++) {
        unsigned len = (*it).first;
        res[len - shortestpath]++;
    }

    for (unsigned i = 0; i < res.size(); i++) {
        cout << "# SP + " << i << " = " << res[i] << endl;
    }
    return res;
}

static vector<double> getAverageDistribution(set_path curpaths, vector<double> avg) {
    vector<double> res = avg;
    vector<unsigned> curdist = pathLengthDistribution(curpaths);
    for (int i = 0; i < (int) curdist.size() - (int) avg.size(); i++) {
        res.push_back(0);
    }
    for (unsigned i = 0; i < curdist.size(); i++)
        res[i] += curdist[i];

    return res;
}

static void experimentsPrecomputedTests(Database &db, Database &dbsketch, char* fileName) {
    ifstream in(fileName);
    map<pair<unsigned, unsigned>, unsigned> pair_dist;
    unsigned u, v, dist;
    ApproxPathEngine appeng;
    while (in >> u) {
        in >> dist;
        in >> v;
        pair<unsigned, unsigned> p(u, v);
        pair_dist[p] = dist;
    }
    vector<double> lengthDistribution;
    unsigned numOfTests = pair_dist.size();
    unsigned noConFound = 0;
    map<unsigned, double> dist_estimation_error, dist_estimation_errorI, dist_estimation_errorII, dist_estimation_error_Tree;
    map<unsigned, unsigned> dist_counter;
    map<double, double> error_distribution, error_distributionI, error_distributionII, error_distribution_Tree;
    map<double, double> error_dist5, error_dist5_I, error_dist5_II, error_dist5_Tree;

    map<double, double> error_dist10, error_dist10_I, error_dist10_II, error_dist10_Tree;
    map<double, double> error_dist15, error_dist15_I, error_dist15_II, error_dist15_Tree;

    unsigned numDist5 = 0, numDist10 = 0, numDist15 = 0;
    cout << "Running " << numOfTests << " tests" << endl;
    double avgNumOfPathsTreeII = 0;

    double avgError = 0, avgErrorAlgoI = 0, avgErrorAlgoII = 0;
    double avgErrorTree = 0, avgErrorTreeI = 0, avgErrorTreeII = 0;
    double avgTime = 0, avgTimeAlgoI = 0, avgTimeAlgoII = 0;


    for (map<pair<unsigned, unsigned>, unsigned>::iterator it = pair_dist.begin(); it != pair_dist.end(); it++) {
/*
        unsigned startInternalNode = (*it).first.first;
        unsigned stopInternalNode = (*it).first.second;
*/
        unsigned startNode = (*it).first.first;
        unsigned stopNode = (*it).first.second;
        cout << "----------------------" << endl;
        cout << "startNode id = " << startNode << endl;
        cout << "stopNode id = " << stopNode << endl;

        Timestamp t1;
        set_path paths;
        appeng.getApproxShortestPath(startNode, stopNode, dbsketch,paths);


        Timestamp t2;
        set_path pathsAlgoI = appeng.modifyPathsI(paths);
        Timestamp t3;
        set_path pathsAlgoII = appeng.modifyPathsII(pathsAlgoI, db);
        Timestamp t4;

//        set_path pathsTree = appeng.getTreeIntersection(startNode, stopNode, dbsketch);

//        set_path pathsTreeI = appeng.modifyPathsI(pathsTree);

        set_path pathsTreeII = appeng.getAdvancedTreeIntersection(startNode, stopNode, dbsketch, db, 0);

        avgNumOfPathsTreeII += pathsTreeII.size();

        vector<unsigned> approxpath, approxAlgoI, approxAlgoII, approxTree, approxTreeI, approxTreeII;
        if (paths.size() == 0) {
            cout << " No connection found!" << endl;
            cout << "Duration: " << (t2 - t1) << " ms" << endl;
            if ((*it).second > 0)
                noConFound++;
        } else {
            approxpath = (*paths.begin()).second;
            cout << "Approximate path (Sketch): ";
            for (unsigned i = 0; i < approxpath.size(); i++)
                cout << approxpath[i] << " ";
            cout << endl;

            approxAlgoI = (*pathsAlgoI.begin()).second;
            cout << "Approximate path (SketchCE): ";
            for (unsigned i = 0; i < approxAlgoI.size(); i++)
                cout << approxAlgoI[i] << " ";
            cout << endl;

            approxAlgoII = (*pathsAlgoII.begin()).second;
            cout << "Approximate path (SketchCESC): ";
            for (unsigned i = 0; i < approxAlgoII.size(); i++)
                cout << approxAlgoII[i] << " ";
            cout << endl;

            approxTreeII = (*pathsTreeII.begin()).second;
            cout << "Approximate path (TreeSketch): ";
            for (unsigned i = 0; i < approxTreeII.size(); i++)
                cout << approxTreeII[i] << " ";
            cout << endl;
        }

        lengthDistribution = getAverageDistribution(pathsAlgoII, lengthDistribution);
        avgTime += t2 - t1;
        avgTimeAlgoI += t3 - t1;
        avgTimeAlgoII += t4 - t1;

        double exactlength = (*it).second;
        double approxlength = approxpath.size() - 1;
        double approxlengthAlgoI = approxAlgoI.size() - 1;
        double approxlengthAlgoII = approxAlgoII.size() - 1;
        double approxlengthTree = approxTree.size() - 1;
        double approxlengthTreeI = approxTreeI.size() - 1;
        double approxlengthTreeII = approxTreeII.size() - 1;

        //        assert(approxlength >= exactlength);
        //        assert(approxlengthAlgoI >= exactlength);
        //        assert(approxlengthAlgoII >= exactlength);
        //        assert(approxlengthTree >= exactlength);
        //        assert(approxlengthTreeI >= exactlength);
        //        assert(approxlengthTreeII >= exactlength);
        cout << "exact length = " << exactlength << ", Sketch = " << approxlength
                << ", SketchCE = " << approxlengthAlgoI << ", SketchCESC = " << approxlengthAlgoII << endl;
        cout << "exact length = " << exactlength <<  ", TreeSketch = " << approxlengthTreeII << endl;
        double error = 0, error1 = 0, error2 = 0, errorTree = 0, errorTreeI = 0, errorTreeII = 0;
        if (approxlength > 1000)
            continue;
        if (exactlength != 0) {
            error = fabs(exactlength - approxlength) / exactlength;
            error1 = fabs(exactlength - approxlengthAlgoI) / exactlength;
            error2 = fabs(exactlength - approxlengthAlgoII) / exactlength;
            errorTree = fabs(exactlength - approxlengthTree) / exactlength;
            errorTreeI = fabs(exactlength - approxlengthTreeI) / exactlength;
            errorTreeII = fabs(exactlength - approxlengthTreeII) / exactlength;

            dist_counter[exactlength]++;
            dist_estimation_error[exactlength] += approxlength;
            dist_estimation_errorI[exactlength] += approxlengthAlgoI;
            dist_estimation_errorII[exactlength] += approxlengthAlgoII;
            dist_estimation_error_Tree[exactlength] += approxlengthTreeII;

            error_distribution[error]++;
            error_distributionI[error1]++;
            error_distributionII[error2]++;
            error_distribution_Tree[errorTreeII]++;

            if (exactlength == 5) {
                numDist5++;
                error_dist5[error]++;
                error_dist5_I[error1]++;
                error_dist5_II[error2]++;
            }
            if (exactlength == 10) {
                numDist10++;
                error_dist10[error]++;
                error_dist10_I[error1]++;
                error_dist10_II[error2]++;
            }
            if (exactlength == 15) {
                numDist15++;
                error_dist15[error]++;
                error_dist15_I[error1]++;
                error_dist15_II[error2]++;
            }
        }
        avgError += error;
        avgErrorAlgoI += error1;
        avgErrorAlgoII += error2;
        avgErrorTree += errorTree;
        avgErrorTreeI += errorTreeI;
        avgErrorTreeII += errorTreeII;

        //        if (error2 > 0)
        //            cout << "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\n";

    }
    avgError /= numOfTests;
    avgErrorAlgoI /= numOfTests;
    avgErrorAlgoII /= numOfTests;
    avgErrorTree /= numOfTests;
    avgErrorTreeI /= numOfTests;
    avgErrorTreeII /= numOfTests;

    avgTime /= numOfTests;
    avgTimeAlgoI /= numOfTests;
    avgTimeAlgoII /= numOfTests;
    avgNumOfPathsTreeII /= numOfTests;
    cout << "-------------------------" << endl;
    cout << "Average ERROR (Sketch)= " << avgError << endl;
    cout << "Average ERROR (SketchCE) = " << avgErrorAlgoI << endl;
    cout << "Average ERROR (SketchCESC) = " << avgErrorAlgoII << endl;
    cout << "Average ERROR (TreeSketch) = " << avgErrorTreeII << endl;
    cout << "No CONNECTION FOUND " << noConFound << " times" << endl;
    cout << "Length Distribution: " << endl;
    double avgPaths = 0;
    for (unsigned i = 0; i < lengthDistribution.size(); i++) {
        lengthDistribution[i] /= numOfTests;
        cout << "# SP +" << i << " = " << lengthDistribution[i] << endl;
        avgPaths += lengthDistribution[i];
    }
    cout << "Average num of paths: " << avgPaths << " " << endl;
    cout << "Average num of paths for TreeSketch: " << avgNumOfPathsTreeII << endl;
    cout << "Error for different lengths: " << endl;
    cout << "Path Length | Sketch | Sketch I | Sketch II | Tree" << endl;
    cout << "----------------------------------------------" << endl;

    for (map<unsigned, double>::iterator it = dist_estimation_error.begin(); it != dist_estimation_error.end(); it++) {
        dist_estimation_error[(*it).first] /= dist_counter[(*it).first];
        dist_estimation_errorI[(*it).first] /= dist_counter[(*it).first];
        dist_estimation_errorII[(*it).first] /= dist_counter[(*it).first];
        dist_estimation_error_Tree[(*it).first] /= dist_counter[(*it).first];

        double error = ((*it).second - (*it).first) / (*it).first;
        double errorI = (dist_estimation_errorI[(*it).first] - (*it).first) / (*it).first;
        double errorII = (dist_estimation_errorII[(*it).first] - (*it).first) / (*it).first;

        double errorTree = (dist_estimation_error_Tree[(*it).first] - (*it).first) / (*it).first;

        cout << "  " << (*it).first << " "
                << error <<
                " " << errorI << " " << errorII << " " << errorTree
                << endl;
    }

    cout << "Error distribution: " << endl;
    cout << "Error | Sketch | SketchCE | SketchCESC | TreeSketch" << endl;
    cout << "----------------------------------------------" << endl;

    for (map<double, double>::iterator it = error_distribution.begin(); it != error_distribution.end(); it++) {
        cout << "   " << (*it).first << " " << (*it).second / numOfTests << " ";
        if (error_distributionI.find((*it).first) == error_distributionI.end())
            cout << "? ";
        else cout << error_distributionI[(*it).first] / numOfTests << " ";

        if (error_distributionII.find((*it).first) == error_distributionII.end())
            cout << "? ";
        else cout << error_distributionII[(*it).first] / numOfTests << " ";

        if (error_distribution_Tree.find((*it).first) == error_distribution_Tree.end())
            cout << "? ";
        else cout << error_distribution_Tree[(*it).first] / numOfTests << " ";

        cout << endl;
    }

/*
    cout << "Error distribution for dist 5: " << endl;
    cout << "Number of paths: " << numDist5 << endl;
    for (map<double, double>::iterator it = error_dist5.begin(); it != error_dist5.end(); it++) {
        cout << "   " << (*it).first << " " << (*it).second / numDist5 << " ";
        if (error_dist5_I.find((*it).first) == error_dist5_I.end())
            cout << "? ";
        else cout << error_dist5_I[(*it).first] / numDist5 << " ";

        if (error_dist5_II.find((*it).first) == error_dist5_II.end())
            cout << "? ";
        else cout << error_dist5_II[(*it).first] / numDist5 << " ";
        cout << endl;
    }

    cout << "Error distribution for dist 10: " << endl;
    cout << "Number of paths: " << numDist10 << endl;
    for (map<double, double>::iterator it = error_dist10.begin(); it != error_dist10.end(); it++) {
        cout << "   " << (*it).first << " " << (*it).second / numDist10 << " ";
        if (error_dist10_I.find((*it).first) == error_dist10_I.end())
            cout << "? ";
        else cout << error_dist10_I[(*it).first] / numDist10 << " ";

        if (error_dist10_II.find((*it).first) == error_dist10_II.end())
            cout << "? ";
        else cout << error_dist10_II[(*it).first] / numDist10 << " ";
        cout << endl;
    }

    cout << "Error distribution for dist 15: " << endl;
    cout << "Number of paths: " << numDist15 << endl;
    for (map<double, double>::iterator it = error_dist15.begin(); it != error_dist15.end(); it++) {
        cout << "   " << (*it).first << " " << (*it).second / numDist15 << " ";
        if (error_dist15_I.find((*it).first) == error_dist15_I.end())
            cout << "? ";
        else cout << error_dist15_I[(*it).first] / numDist15 << " ";

        if (error_dist15_II.find((*it).first) == error_dist15_II.end())
            cout << "? ";
        else cout << error_dist15_II[(*it).first] / numDist15 << " ";
        cout << endl;
    }
*/
}

int main(int argc, char* argv[]) {
    if (argc < 4) {
        cout << "usage: " << argv[0] << " <rdfstore>  <sketch store> <tests file> " << endl;
        return 1;
    }

    // Open the database
    Database db;
    if (!db.open(argv[1])) {
        cout << "unable to open " << argv[1] << endl;
        return 1;
    }

    Database dbsketch;
    if (!dbsketch.open(argv[2])) {
        cout << "unable to open " << argv[2] << endl;
        return 1;

    }

    Timestamp t1;
    experimentsPrecomputedTests(db, dbsketch, argv[3]);
    Timestamp t2;


    db.close();
    dbsketch.close();
    cout << "Duration: " << (t2 - t1) << " ms" << endl;
}


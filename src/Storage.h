#include <iostream>
#include <cstdlib>
#include <string>
#include <cstring>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <thread>
#include <vector>
#include <sstream>
#include <algorithm>
#include <map>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <unordered_map>
#include <cmath>
#include <queue>
#include <set>

using namespace std;
extern bool isMaster;

struct ClientState
{
    bool multi = false;
    vector<vector<string>> queue;
    int execCounts = 0;
    vector<string> replyQueue;
    bool executingTransaction = false;
    set<string> watchedKeys;
    bool watchedKeyModified = false;
};

extern set<int> replicaFds;
extern mutex replicasMutex;

extern unordered_map<int, ClientState> clients;
extern mutex clientsMutex;

bool isNumber(const string &str);
extern int masterConnectionFd;
class ListStorage
{
private:
    unordered_map<string, chrono::steady_clock::time_point> ExpiryTimes;
    unordered_map<string, string> Database;
    unordered_map<string, vector<string>> List;
    map<string, vector<pair<string, map<string, string>>>> Streams;
    mutex mtx;
    condition_variable cv;

public:
    void handlePing(vector<string> &cmd, int client_fd);
    void handleEcho(vector<string> &cmd, int client_fd);
    void handleSET(vector<string> &cmd, int client_fd);
    void handleGET(vector<string> &cmd, int client_fd);
    void handleRPUSH(vector<string> &cmd, int client_fd);
    void handleLPUSH(vector<string> &cmd, int client_fd);
    void handleLRANGE(vector<string> &cmd, int client_fd);
    void handleLLEN(vector<string> &cmd, int client_fd);
    void handleLPOP(vector<string> &cmd, int client_fd);
    void handleBLPOP(vector<string> &cmd, int client_fd);
    void handleTYPE(vector<string> &cmd, int client_fd);
    void handleXADD(vector<string> &cmd, int client_fd);
    void handleXRANGE(vector<string> &cmd, int client_fd);
    void handleXREAD(vector<string> &cmd, int client_fd);
    void handleXREAD_BLOCK(vector<string> &cmd, int client_fd);
    void handleINCR(vector<string> &cmd, int client_fd);
    void handleQueuing(vector<string> &cmd, int client_fd);
    void dispatch(vector<string> &cmd, int client_fd);
    void handleMULTI(vector<string> &cmd, int client_fd);
    void handleEXEC(vector<string> &cmd, int client_fd);
    void handleDISCARD(vector<string> &cmd, int client_fd);
    void handleWATCH(vector<string> &cmd, int client_fd);
    void handleUNWATCH(vector<string> &cmd, int client_fd);
    void handleInfoReplication(vector<string> &cmd, int client_fd);
    void handleREPLCONF(vector<string> &cmd, int client_fd);
    void handlePSYNC(vector<string> &cmd, int client_fd);
    void propagateToReplicas(const string &respEncodedCommand);
    void handleWAIT(vector<string> &cmd, int client_fd);
    void configGetCommand(vector<string> &cmd, int client_fd);
    void key_RDB(vector<string> &cmd, int client_fd);
    void loadRDB();


};

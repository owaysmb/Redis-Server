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

using namespace std;

bool isNumber(const string &str);

class ListStorage
{
private:
    unordered_map<string, chrono::steady_clock::time_point> ExpiryTimes;
    unordered_map<string, string> Database;
    unordered_map<string, vector<string>> List;
    map<string, vector<pair<string, map<string, string>>>> Streams;
    mutex mtx;
    condition_variable cv;
    vector<vector<string>> Q;
    int ExecCounts = 0;
    bool Multi = false;
    string replyBuffer;
    bool capturingMode = false;

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
};

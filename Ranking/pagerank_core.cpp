/**
 * VELO Search - C++ PageRank Algorithm
 * Fast PageRank computation and search indexing
 * Compile: g++ -O3 -shared -o pagerank_core.dll pagerank_core.cpp
 */

#include <cstring>
#include <cstdlib>
#include <vector>
#include <string>
#include <algorithm>
#include <cctype>
#include <cmath>
#include <map>
#include <set>

#ifdef _WIN32
    #define EXPORT __declspec(dllexport)
#else
    #define EXPORT __attribute__((visibility("default")))
#endif

// PageRank Node structure
struct PageNode {
    int id;
    char url[1024];
    float rank;
    int outLinks;
    std::vector<int> inLinks;
};

// Graph for PageRank computation
class PageRankGraph {
private:
    std::vector<PageNode> nodes;
    std::map<std::string, int> urlToId;
    std::map<int, std::vector<int>> adjacencyList;
    
    static const float DAMPING_FACTOR;
    static const int MAX_ITERATIONS;
    static const float CONVERGENCE_THRESHOLD;
    
public:
    EXPORT PageRankGraph() {}
    
    EXPORT void addPage(int id, const char* url) {
        PageNode node;
        node.id = id;
        strncpy(node.url, url, 1023);
        node.url[1023] = '\0';
        node.rank = 1.0f;
        node.outLinks = 0;
        
        nodes.push_back(node);
        urlToId[std::string(url)] = id;
    }
    
    EXPORT void addLink(int fromId, int toId) {
        adjacencyList[fromId].push_back(toId);
        
        // Update outLinks count
        for (auto& node : nodes) {
            if (node.id == fromId) {
                node.outLinks++;
                break;
            }
        }
        
        // Update inLinks
        for (auto& node : nodes) {
            if (node.id == toId) {
                node.inLinks.push_back(fromId);
                break;
            }
        }
    }
    
    EXPORT void computePageRank() {
        int n = nodes.size();
        if (n == 0) return;
        
        std::vector<float> newRanks(n, 1.0f / n);
        std::vector<float> oldRanks(n, 1.0f / n);
        
        for (int iter = 0; iter < MAX_ITERATIONS; iter++) {
            float diff = 0.0f;
            
            for (int i = 0; i < n; i++) {
                float rank = (1.0f - DAMPING_FACTOR) / n;
                
                // Sum contributions from incoming links
                for (int fromId : nodes[i].inLinks) {
                    // Find node with fromId
                    for (int j = 0; j < n; j++) {
                        if (nodes[j].id == fromId && nodes[j].outLinks > 0) {
                            rank += DAMPING_FACTOR * oldRanks[j] / nodes[j].outLinks;
                            break;
                        }
                    }
                }
                
                newRanks[i] = rank;
                diff += std::abs(newRanks[i] - oldRanks[i]);
            }
            
            // Check convergence
            if (diff < CONVERGENCE_THRESHOLD) {
                break;
            }
            
            oldRanks = newRanks;
        }
        
        // Update node ranks
        for (int i = 0; i < n; i++) {
            nodes[i].rank = newRanks[i];
        }
    }
    
    EXPORT float getPageRank(int id) {
        for (const auto& node : nodes) {
            if (node.id == id) {
                return node.rank;
            }
        }
        return 0.0f;
    }
    
    EXPORT int getNodeCount() {
        return nodes.size();
    }
    
    EXPORT void clear() {
        nodes.clear();
        urlToId.clear();
        adjacencyList.clear();
    }
};

const float PageRankGraph::DAMPING_FACTOR = 0.85f;
const int PageRankGraph::MAX_ITERATIONS = 100;
const float PageRankGraph::CONVERGENCE_THRESHOLD = 0.0001f;

// TF-IDF Scoring
class TFIDFScorer {
public:
    EXPORT static float computeTF(const char* term, const char* document) {
        std::string doc(document);
        std::string t(term);
        
        // Convert to lowercase
        std::transform(doc.begin(), doc.end(), doc.begin(), ::tolower);
        std::transform(t.begin(), t.end(), t.begin(), ::tolower);
        
        // Count occurrences
        int count = 0;
        size_t pos = 0;
        while ((pos = doc.find(t, pos)) != std::string::npos) {
            count++;
            pos += t.length();
        }
        
        // Term frequency
        int totalTerms = doc.length() > 0 ? doc.length() / 4 : 1; // Approximate
        return (float)count / totalTerms;
    }
    
    EXPORT static float computeIDF(const char* term, int totalDocs, int docsWithTerm) {
        if (docsWithTerm == 0) return 0.0f;
        return log((float)totalDocs / (float)docsWithTerm) + 1.0f;
    }
};

// Fast Inverted Index with PageRank boost
class HybridSearchIndex {
private:
    std::map<std::string, std::vector<std::pair<int, float>>> index;
    std::vector<std::string> documents;
    PageRankGraph* pageRankGraph;
    
    std::string toLower(const char* str) {
        std::string result(str);
        std::transform(result.begin(), result.end(), result.begin(), ::tolower);
        return result;
    }
    
public:
    EXPORT HybridSearchIndex() {
        pageRankGraph = new PageRankGraph();
    }
    
    EXPORT ~HybridSearchIndex() {
        delete pageRankGraph;
    }
    
    EXPORT void addDocument(int id, const char* title, const char* url, const char* content) {
        // Add to PageRank graph
        pageRankGraph->addPage(id, url);
        
        // Index terms
        std::string fullText = std::string(title) + " " + std::string(content);
        std::string lowerText = toLower(fullText.c_str());
        
        char* buffer = new char[lowerText.length() + 1];
        strcpy(buffer, lowerText.c_str());
        
        char* token = strtok(buffer, " \t\n.,!?;:");
        while (token != nullptr) {
            std::string term(token);
            if (term.length() > 2) {
                // Compute TF for this term in this document
                float tf = TFIDFScorer::computeTF(term.c_str(), fullText.c_str());
                
                // Add to index
                index[term].push_back({id, tf});
            }
            token = strtok(nullptr, " \t\n.,!?;:");
        }
        
        delete[] buffer;
        documents.push_back(fullText);
    }
    
    EXPORT void addLink(int fromId, int toId) {
        pageRankGraph->addLink(fromId, toId);
    }
    
    EXPORT void computePageRank() {
        pageRankGraph->computePageRank();
    }
    
    EXPORT int search(const char* query, int* resultIds, float* scores, int maxResults) {
        std::string q = toLower(query);
        std::map<int, float> docScores;
        
        // Parse query terms
        char* buffer = new char[q.length() + 1];
        strcpy(buffer, q.c_str());
        
        char* token = strtok(buffer, " \t\n.,!?;:");
        while (token != nullptr) {
            std::string term(token);
            if (term.length() > 2) {
                auto it = index.find(term);
                if (it != index.end()) {
                    for (const auto& pair : it->second) {
                        int docId = pair.first;
                        float tf = pair.second;
                        
                        // Simple IDF (approximate)
                        int docsWithTerm = it->second.size();
                        float idf = TFIDFScorer::computeIDF(term.c_str(), documents.size(), docsWithTerm);
                        
                        // TF-IDF score
                        float tfidf = tf * idf;
                        
                        // Add PageRank boost
                        float pr = pageRankGraph->getPageRank(docId);
                        float boost = 1.0f + pr * 10.0f; // PageRank boost
                        
                        docScores[docId] += tfidf * boost;
                    }
                }
            }
            token = strtok(nullptr, " \t\n.,!?;:");
        }
        
        delete[] buffer;
        
        // Sort by score
        std::vector<std::pair<float, int>> sortedScores;
        for (const auto& pair : docScores) {
            sortedScores.push_back({pair.second, pair.first});
        }
        
        std::sort(sortedScores.begin(), sortedScores.end(), 
            [](const std::pair<float, int>& a, const std::pair<float, int>& b) {
                return a.first > b.first;
            });
        
        // Return top results
        int count = 0;
        for (const auto& pair : sortedScores) {
            if (count >= maxResults) break;
            resultIds[count] = pair.second;
            scores[count] = pair.first;
            count++;
        }
        
        return count;
    }
    
    EXPORT void clear() {
        index.clear();
        documents.clear();
        pageRankGraph->clear();
    }
    
    EXPORT int getDocumentCount() {
        return documents.size();
    }
};

// C API for Python ctypes
extern "C" {
    // PageRank Graph API
    EXPORT PageRankGraph* createPageRankGraph() {
        return new PageRankGraph();
    }
    
    EXPORT void destroyPageRankGraph(PageRankGraph* graph) {
        delete graph;
    }
    
    EXPORT void graphAddPage(PageRankGraph* graph, int id, const char* url) {
        if (graph) graph->addPage(id, url);
    }
    
    EXPORT void graphAddLink(PageRankGraph* graph, int fromId, int toId) {
        if (graph) graph->addLink(fromId, toId);
    }
    
    EXPORT void graphComputePageRank(PageRankGraph* graph) {
        if (graph) graph->computePageRank();
    }
    
    EXPORT float graphGetPageRank(PageRankGraph* graph, int id) {
        return graph ? graph->getPageRank(id) : 0.0f;
    }
    
    // Hybrid Search Index API
    EXPORT HybridSearchIndex* createHybridIndex() {
        return new HybridSearchIndex();
    }
    
    EXPORT void destroyHybridIndex(HybridSearchIndex* index) {
        delete index;
    }
    
    EXPORT void hybridAddDocument(HybridSearchIndex* index, int id, const char* title, 
                                   const char* url, const char* content) {
        if (index) index->addDocument(id, title, url, content);
    }
    
    EXPORT void hybridAddLink(HybridSearchIndex* index, int fromId, int toId) {
        if (index) index->addLink(fromId, toId);
    }
    
    EXPORT void hybridComputePageRank(HybridSearchIndex* index) {
        if (index) index->computePageRank();
    }
    
    EXPORT int hybridSearch(HybridSearchIndex* index, const char* query, 
                            int* resultIds, float* scores, int maxResults) {
        if (index) {
            return index->search(query, resultIds, scores, maxResults);
        }
        return 0;
    }
    
    EXPORT void hybridClear(HybridSearchIndex* index) {
        if (index) index->clear();
    }
    
    EXPORT int hybridGetDocCount(HybridSearchIndex* index) {
        return index ? index->getDocumentCount() : 0;
    }
    
    // TF-IDF API
    EXPORT float computeTF(const char* term, const char* document) {
        return TFIDFScorer::computeTF(term, document);
    }
    
    EXPORT float computeIDF(const char* term, int totalDocs, int docsWithTerm) {
        return TFIDFScorer::computeIDF(term, totalDocs, docsWithTerm);
    }
}

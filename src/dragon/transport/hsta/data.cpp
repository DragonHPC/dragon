#include "agent.hpp"
#include "data.hpp"

// TODO: need to update this to support multi-threading

static FILE *perf_file = nullptr;
static const char *section_separator = ">>>======================================================================<<<";

template <typename T>
DataSet<T>::DataSet()
{
    this->ppn = hsta_my_agent->network.ppn;
}

template <typename T>
DataSet<T>::DataSet(std::string name, T val)
{
    this->name                = name;
    this->ppn = hsta_my_agent->network.ppn;

    if (perf_file == nullptr) {
        perf_file = fopen("hsta_perf.log", "a");
        hsta_dbg_assert(perf_file != nullptr);
    }

    this->values.resize(hsta_my_agent->network.num_ranks);
    this->values[hsta_my_agent->network.rank] = val;

    [[maybe_unused]] T *values_buf = this->values.data();
    // TODO: no allgather at the moment
    // hsta_my_agent->network.allgather(&val, values_buf, sizeof(T));
}

template <typename T>
void DataSet<T>::analyze_and_log()
{
    fprintf(perf_file, "PERFORMANCE ANALYSIS: %s\n", this->name.c_str());

    auto mean     = this->get_mean();
    auto min_val  = this->get_min();
    auto max_val  = this->get_max();
    auto variance = this->get_variance();
    auto std_dev  = this->get_std_dev();

    fprintf(perf_file,
            "> mean/min/max = %lf/%lf/%lf\n",
            static_cast<double>(mean),
            static_cast<double>(min_val),
            static_cast<double>(max_val));

    fprintf(perf_file,
            "> variance     = %lf\n\n",
            static_cast<double>(variance));

    // find nodes with outlying values

    std::unordered_map<int, T> nodes_w_outlying_value;

    auto i = 0;

    for (auto nid = 0; nid < hsta_my_agent->network.num_nodes; ++nid) {
        for (auto lrank = 0; lrank < this->ppn; ++lrank) {
            auto val = this->values[i++];
            if (abs(val - mean) > std_dev) {
                nodes_w_outlying_value[nid] = val;
            }
        }
    }

    if (!nodes_w_outlying_value.empty()) {
        fprintf(perf_file,
                "CHECK FOR OUTLIERS BY VALUE:\n"
                "> A value is an outlier if the difference between the value and the mean\n"
                "> (= %lf) is greater than 1 standard devation (= %lf).\n\n"
                "Nodes with outlying values:\n",
                mean, std_dev);

        for (auto nid_val : nodes_w_outlying_value) {
            auto nid = nid_val.first;
            auto val  = static_cast<double>(nid_val.second);

            fprintf(perf_file, "  > nid %d: value = %lf\n", nid, val);
        }
    } else {
        fprintf(perf_file,
                "CHECK FOR OUTLIERS BY VALUE:\n"
                "> A value is an outlier if the difference between the value and the mean\n"
                "> (= %lf) is greater than 1 standard devation (= %lf).\n\n"
                "No nodes with outlying values\n",
                mean, std_dev);
    }

    fprintf(perf_file, "\n");
    nodes_w_outlying_value.clear();

    // find nodes with outlying variance *within the node*

    std::unordered_map<int, double> nodes_w_outlying_variance;
    DataSet<double> intra_node_variance;

    for (auto nid = 0; nid < hsta_my_agent->network.num_nodes; ++nid) {
        auto variance = this->get_variance_on_node(nid);
        intra_node_variance.add_value(variance);
    }

    auto mean_variance    = intra_node_variance.get_mean();
    auto std_dev_variance = intra_node_variance.get_std_dev();

    for (auto nid = 0; nid < hsta_my_agent->network.num_nodes; ++nid) {
        auto variance = intra_node_variance.values[nid];
        if (abs(variance - mean_variance) > std_dev_variance) {
            nodes_w_outlying_variance[nid] = variance;
        }
    }

    if (!nodes_w_outlying_variance.empty()) {
        fprintf(perf_file,
                "CHECK FOR OUTLIERS BY INTRA-NODE VARIANCE:\n"
                "> A variance value is an outlier if the difference between the intra-node\n"
                "> variance and the mean of intra-node variances (= %lf) is greater than 1\n"
                "> standard devation (= %lf).\n\n"
                "Nodes with outlying variance values:\n",
                mean_variance, std_dev_variance);

        for (auto nid_variance : nodes_w_outlying_variance) {
            auto nid = nid_variance.first;
            auto variance = nid_variance.second;

            fprintf(perf_file, "  > nid %d: variance = %lf, with values ", nid, variance);
            for (auto lrank = 0; lrank < this->ppn; ++lrank) {
                auto base_idx = nid * this->ppn;
                auto val = this->values[base_idx + lrank];
                fprintf(perf_file, "%lf ", static_cast<double>(val));
            }
            fprintf(perf_file, "\n");
        }
    } else {
        fprintf(perf_file,
                "CHECK FOR OUTLIERS BY INTRA-NODE VARIANCE:\n"
                "> A variance value is an outlier if the difference between the intra-node\n"
                "> variance and the mean of intra-node variances (= %lf) is greater than 1\n"
                "> standard devation (= %lf).\n\n"
                "No nodes with outlying variance values\n",
                mean_variance, std_dev_variance);
    }

    fprintf(perf_file, "\n");
    nodes_w_outlying_variance.clear();

    fprintf(perf_file, "\n\n%s\n\n\n", section_separator);
    fflush(perf_file);
}

template class DataSet<double>;
template class DataSet<uint64_t>;

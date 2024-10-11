#ifndef DATA_HPP
#define DATA_HPP

#include <math.h>
#include "extras.hpp"
#include "utils.hpp"

template <typename T>
class DataSet {
private:

    std::string name;
    int ppn;

public:

    std::vector<T> values;

    DataSet();

    // this constructor is collective, in the sense that it does an
    // allgather operation to populate "values" and so needs to be
    // called in the correct order
    DataSet(std::string name, T val);

    size_t size()
    {
        return this->values.size();
    }

    void add_value(T val)
    {
        this->values.push_back(val);
    }

    void merge_dataset(DataSet& data_to_merge)
    {
        this->values.resize(data_to_merge.size());

        for (auto new_val : data_to_merge.values) {
            this->add_value(new_val);
        }
    }

    void clear()
    {
        this->values.clear();
    }

    T get_min()
    {
        auto min_val = std::numeric_limits<T>::max();

        for (auto val : this->values) {
            if (val < min_val) {
                min_val = val;
            }
        }

        return min_val;
    }

    T get_min_on_node(int nid)
    {
        auto min_val = std::numeric_limits<T>::max();
        int base_idx = nid * this->ppn;

        for (auto lrank = 0; lrank < this->ppn; ++lrank) {
            auto val = this->values[base_idx + lrank];
            if (val < min_val) {
                min_val = val;
            }
        }

        return min_val;
    }

    T get_max()
    {
        auto max_val = std::numeric_limits<T>::min();

        for (auto val : this->values) {
            if (val > max_val) {
                max_val = val;
            }
        }

        return max_val;
    }

    T get_max_on_node(int nid)
    {
        auto max_val = std::numeric_limits<T>::min();
        int base_idx = nid * this->ppn;

        for (auto lrank = 0; lrank < this->ppn; ++lrank) {
            auto val = this->values[base_idx + lrank];
            if (val > max_val) {
                max_val = val;
            }
        }

        return max_val;
    }

    double get_mean()
    {
        double sum = 0.0;

        for (auto val : this->values) {
            sum += val;
        }

        return sum / static_cast<double>(this->size());
    }

    double get_mean_on_node(int nid)
    {
        double sum   = 0.0;
        int base_idx = nid * this->ppn;

        for (auto lrank = 0; lrank < this->ppn; ++lrank) {
            sum += this->values[base_idx + lrank];
        }

        return sum / static_cast<double>(this->size());
    }

    double get_variance()
    {
        auto mean       = this->get_mean();
        double variance = 0.0;

        for (auto val : this->values) {
            auto diff = val - mean;
            variance += diff * diff;
        }

        return variance / static_cast<double>(this->size());
    }

    double get_variance_on_node(int nid)
    {
        auto mean       = this->get_mean_on_node(nid);
        double variance = 0.0;
        int base_idx    = nid * this->ppn;

        for (auto lrank = 0; lrank < this->ppn; ++lrank) {
            auto diff = this->values[base_idx + lrank] - mean;
            variance += diff * diff;
        }

        return variance / static_cast<double>(this->size());
    }

    double get_std_dev()
    {
        return sqrt(this->get_variance());
    }

    double get_std_dev_on_node(int nid)
    {
        return sqrt(this->get_variance_on_node(nid));
    }

    void analyze_and_log();
};

#endif // !DATA_HPP
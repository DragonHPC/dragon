#include <iostream>
#include <cstdlib>
#include <ctime>
#include <cmath>
#include<unistd.h> 

int main(int argc, char* argv[]) {
    if (argc != 2) {
        return 1;
    }

    int sleep_time = std::atoi(argv[1]);

    std::srand(std::time(nullptr)); // seed the random number generator

    std::cout << "Sleeping for " << sleep_time << " seconds..." << std::endl;
    sleep(sleep_time);
    
    double random_num = std::rand() / static_cast<double>(RAND_MAX) * 10.0;
    std::cout<<random_num<<std::endl;

    if (random_num < 8.0) {
        return 0;
    } else {
        return std::floor(random_num);
    }
}

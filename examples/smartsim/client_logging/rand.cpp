#include <iostream>
#include <cstdlib>
#include <ctime>
#include <cmath>

int main(int argc, char* argv[]) {
    std::srand(std::time(nullptr)); 
    double random_num = std::rand() / static_cast<double>(RAND_MAX) * 10.0;
    std::cout<<random_num<<std::endl;
    return 0;
}

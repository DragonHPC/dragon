#include <stdio.h>
#include <stdlib.h>

int main(int argc, char *argv[]) {
        char *a = argv[1];
        char *b = argv[2];
		double c = atof(a);
		double d = atof(b);
        double result = (c*c) + (d*d);
		if (result < 1) {
			printf("True");
		} else {
			printf("False");
		}
        return 0;
}


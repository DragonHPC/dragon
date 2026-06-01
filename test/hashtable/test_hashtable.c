#include "../../src/lib/_hashtable.h"
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <stdbool.h>

#include "../_ctest_utils.h"

#define TRUE 1
#define FALSE 0

uint64_t data[] = {34616,57095,2025,39548,85153,44329,79023,51552,71593,71361,97845,69481,26284,70490,41867,45284,84451,15937,85361,89752,66545,
                    40868,78118,89108,42927,78169,16969,87047,76714,63523,87525,92105,29014,9011,40676,79289,15953,52149,67425,41747,91109,22760,
                    99876,43284,63293,35284,14949,1830,44705,46408,40952,73604,32183,12142,86960,41335,58312,18598,34247,96269,9013,15607,7052,
                    83084,81989,72931,68277,73037,18063,75493,38143,41579,28159,92365,76843,33544,1452,61285,20270,48732,84638,98164,66115,50200,
                    36262,19552,69027,59948,69109,75884,56477,96024,1334,52066,18291,93475,15642,86890,75315,71145,82966,75113,75022,4666,70661,
                    64836,97746,78754,39045,17188,50302,852,66823,43366,60380,92075,49358,14242,94274,13597,62233,39707,40181,73817,44917,56958,
                    75653,19081,75701,36874,62354,31706,52192,60270,66746,15847,41359,92275,73063,38710,17834,7755,89315,93128,25172,4441,36948,
                    11749,50116,12689,32928,48941,70555,27440,19679,69009,62994,58479,13547,44313,42810,34458,12805,98829,61032,5635,81720,49659,
                    48456,65128,30443,41770,38472,40902,70237,39100,67101,27511,92445,36921,18791,55242,39193,88144,13779,47528,30120,38987,92670,
                    57062,9375,56823,62636,16200,32512,78808,62234,25911,60463,84047,32178,78805,56654,49143,62670,67546,12223,6772,27598,53101,
                    16383,78772,88605,96523,29238,57287,22879,66734,4687,55348,9179,52381,72396,95994,50923,26497,60806,14922,70378,69542,88752,
                    75541,73130,83439,80609,68697,45027,56458,52403,28738,71060,37348,43806,41373,45479,83665,20216,7197,10941,42745,31510,97157,
                    61131,3776,77855,27262,6593,10498,21228,11960,88940,70339,26534,36791,56974,31618,69746,85529,71659,31053,51735,7860,97629,
                    90143,66129,99532,78658,48981,82239,34269,87107,96664,32281,50421,13492,98875,914,96675,9747,62121,98326,75693,96491,25020,
                    12181,57359,63695,49685,96160,88920,37278,68748,52155,67136,69602,15594,96174,23472,67953,41423,29176,41645,41289,49529,
                    74880,88222,99726,92065,97904,61164,31535,8826,7674,45320,81284,19709,50055,13715,80409,36628,22698,461,71089,10963,59711,
                    43510,18060,92342,66949,46952,95860,51805,69430,64022,69436,73824,14469,14837,33147,14106,52463,70737,95859,69057,78016,
                    98511,38315,93026,90655,55586,22922,19658,74150,24606,49194,70901,97334,29445,51881,18429,26570,45643,43960,53705,28582,
                    21478,54882,62927,55228,99864,12300,50885,61115,18950,25598,29376,44015,95077,79922,64528,78168,61404,28523,66188,14397,
                    387,20446,54565,87598,33102,58773,62464,59749,50564,94368,14671,29189,38755,255,32792,28632,38378,75229,58437,1924,49618,
                    78024,93529,4755,93560,52215,1513,26828,71092,17356,24664,92465,94231,35244,84808,22989,75501,19795,54668,88031,19639,
                    10666,76043,29328,76412,17354,44703,73962,82059,13322,9728,14905,3750,83303,14804,10250,27408,6511,73682,34811,14222,
                    62809,33870,90365,22904,70637,70383,87506,53958,42454,17594,73957,96253,40562,52460,72294,10859,28784,12263,60874,69509,
                    96891,27310,31354,17035,23820,2056,52164,68307,85327,50796,83583,39917,96375,33958,52159,81571,25166,5459,64647,36067,
                    85436,69496,31784,68480,9000,93946,44983,42786,50089,39436,41115,13130,96737,2447};

typedef struct dragonMemoryManifestRec_st {
    uint64_t alloc_type; // key part 1
    uint64_t alloc_type_id; // key part 2
    uint64_t offset; // value part 1
    uint64_t size; // value part 2
} dragonMemoryManifestRec_t;

typedef struct keyvalue_st {
    uint64_t key;
    uint64_t value;
} keyvalue_t;

int main(int argc, char* argv[]) {
    uint64_t size;
    dragonError_t rc;
    size_t tests_passed = 0;
    size_t tests_attempted = 0;
    bool test_passed = false;
    dragonHashtableStats_t stats;


    printf("Test Case 1: Test size function\n");
    tests_attempted = tests_attempted + 1;
    test_passed = false;

    rc = dragon_hashtable_size(500, sizeof(uint64_t), sizeof(uint64_t), &size);

    if (rc != DRAGON_SUCCESS) {
        printf("Error Code was %u\n",rc);
    } else {
        tests_passed += 1;
    }

    void* hashtable_ptr = malloc(size);


    printf("Test Case 2: Test init function\n");
    tests_attempted = tests_attempted + 1;
    test_passed = false;


    dragonHashtable_t tblHandle;

    rc = dragon_hashtable_init(hashtable_ptr, &tblHandle, 500, sizeof(uint64_t), sizeof(uint64_t));

    if (rc != DRAGON_SUCCESS) {
        printf("Error Code was %u\n",rc);
    } else {
        tests_passed += 1;

    }

    printf("Test Case 3: Test add function\n");
    tests_attempted = tests_attempted + 1;
    test_passed = false;

    keyvalue_t kv;

    kv.key = 42;
    kv.value = 43;

    rc = dragon_hashtable_add(&tblHandle, (char*)&kv.key, (char*)&kv.value);

    if (rc != DRAGON_SUCCESS) {
        printf("Error Code was %u\n",rc);
    } else {
        tests_passed += 1;
    }

    printf("Test Case 4: Test get function\n");
    tests_attempted = tests_attempted + 1;
    test_passed = true;

    uint64_t val;

    rc = dragon_hashtable_get(&tblHandle, (char*)&kv.key, (char*)&val);

    if (rc != DRAGON_SUCCESS) {
        printf("Error Code was %u\n",rc);
        test_passed = false;
    }

    if (val != kv.value) {
        printf("Error: values did not match %lu and %lu\n", kv.value, val);
        test_passed = false;
    }

    if (test_passed)
        tests_passed += 1;

    printf("Test Case 5: Attach a new handle\n");
    tests_attempted = tests_attempted + 1;
    dragonHashtable_t tblHandle2;

    rc = dragon_hashtable_attach(hashtable_ptr, &tblHandle2);

    if (rc != DRAGON_SUCCESS) {
        printf("Error Code was %u\n",rc);
    } else {
        tests_passed += 1;
    }

    printf("Test Case 6: Test remove function\n");
    tests_attempted = tests_attempted + 1;

    rc = dragon_hashtable_remove(&tblHandle2, (char*)&kv.key);

    if (rc != DRAGON_SUCCESS) {
        printf("Error Code was %u\n",rc);
    } else {
        tests_passed += 1;
    }

    printf("Test Case 7: Test detach function\n");
    tests_attempted = tests_attempted + 1;

    rc = dragon_hashtable_detach(&tblHandle2);

    if (rc != DRAGON_SUCCESS) {
        printf("Error Code was %u\n",rc);
    } else {
        tests_passed += 1;
    }

    printf("Test Case 8: Test get function for non-key\n");
    tests_attempted = tests_attempted + 1;

    rc = dragon_hashtable_get(&tblHandle, (char*)&kv.key, (char*)&val);

    if (rc != DRAGON_HASHTABLE_KEY_NOT_FOUND) {
        printf("Error Code was %u\n",rc);
    } else {
        tests_passed += 1;
    }

    size_t data_len = sizeof(data) / sizeof(data[0]);

    printf("Test Case 9: Add %lu entries to the hashtable\n", data_len);
    tests_attempted = tests_attempted + 1;
    test_passed = true;

    size_t rejected = 0;

    for (int i=0;i<data_len;i++) {
        val = data[i] + 1;
        rc = dragon_hashtable_add(&tblHandle, (char*)&data[i],(char*)&val);

        if (rc == DRAGON_HASHTABLE_FULL) {
            rejected = rejected + 1;
        } else if (rc != DRAGON_SUCCESS) {
            printf("Error Code was %u\n",rc);
            test_passed = false;
        }
    }

    if (test_passed && rejected == 10)
        tests_passed += 1;


    printf("Test Case 10: Replace 10 entries in the hashtable\n");
    tests_attempted = tests_attempted + 1;
    test_passed = true;

    for (int i=0;i<10;i++) {
        val = data[i] + 2;
        rc = dragon_hashtable_replace(&tblHandle, (char*)&data[i],(char*)&val);

        if (rc != DRAGON_SUCCESS) {
            printf("Error Code was %u\n",rc);
            test_passed = false;
        }
    }

    if (test_passed)
        tests_passed += 1;

    printf("Test Case 11: Lookup %lu entries in the hashtable\n", data_len);
    tests_attempted = tests_attempted + 1;
    test_passed = true;

    rejected = 0;

    for (int i=0;i<data_len;i++) {
        rc = dragon_hashtable_get(&tblHandle, (char*)&data[i],(char*)&val);
        if (rc == DRAGON_HASHTABLE_KEY_NOT_FOUND) {
            rejected = rejected + 1;
        } else if (rc == DRAGON_SUCCESS) {
            if (i >= 10 && val != data[i]+1) {
                printf("There was a problem looking up key %lu in the hashtable. Value should have been %lu and it was %lu instead.\n", data[i], data[i]+1, val);
                test_passed = false;
            }
            if (i < 10 && val != data[i]+2) {
                printf("There was a problem looking up key %lu in the hashtable. Value should have been %lu and it was %lu instead.\n", data[i], data[i]+2, val);
                test_passed = false;
            }
        } else if (rc != DRAGON_SUCCESS) {
            printf("Error Code was %u\n",rc);
        }
    }

    if (test_passed && rejected == 10) {
        tests_passed += 1;
    }

    //rc = dragon_hashtable_dump("Final Hashtable State Dump\n=================================", &tblHandle, "");

    printf("Test Case 12: Remove half the entries from the hashtable\n");
    tests_attempted += 1;
    test_passed = true;

    for (int i=0;i<data_len-rejected;i++) {
        if (i%2==0) {
            rc = dragon_hashtable_remove(&tblHandle, (char*)&data[i]);
            if (rc != DRAGON_SUCCESS) {
                test_passed = false;
                printf("Error Code on remove was %u\n",rc);
            }
        }
    }

    if (test_passed) {
        tests_passed += 1;
    }

    printf("Test Case 13: Check membership for remaining entries\n");
    tests_attempted += 1;

    test_passed = true;

    for (int i=0;i<data_len-rejected;i++) {
        if (i%2==1) {
            rc = dragon_hashtable_get(&tblHandle, (char*)&data[i], (char*)&val);
            if (rc != DRAGON_SUCCESS) {
                test_passed = false;
                printf("Error Code on get was %u\n",rc);
            }
            if (i >= 10 && val != data[i]+1) {
                printf("There was a problem looking up key %lu in the hashtable. Value should have been %lu and it was %lu instead.\n", data[i], data[i]+1, val);
                test_passed = false;
            }
            if (i < 10 && val != data[i]+2) {
                printf("There was a problem looking up key %lu in the hashtable. Value should have been %lu and it was %lu instead.\n", data[i], data[i]+2, val);
                test_passed = false;
            }
        }
    }

    if (test_passed) {
        tests_passed += 1;
    }

    //rc = dragon_hashtable_dump("Final Hashtable State Dump\n=================================", &tblHandle, "");

    printf("Test Case 14: Test Iterator\n");
    tests_attempted += 1;
    test_passed = true;

    dragonHashtableIterator_t iter;

    rc = dragon_hashtable_iterator_init(&tblHandle, &iter);

    if (rc != DRAGON_SUCCESS) {
        test_passed = false;
        printf("Error Code on iterator_init was %u\n",rc);
    } else {
        int count = 0;
        while (rc != DRAGON_HASHTABLE_ITERATION_COMPLETE) {
            rc = dragon_hashtable_iterator_next(&tblHandle, &iter, (char*)&kv.key, (char*)&kv.value);
            if (rc == DRAGON_SUCCESS) {
                count++;
                //printf("%lu: %lu\n", kv.key, kv.value);
            }
        }

        if (count != (data_len-rejected)/2) {
            test_passed = false;
            printf("Error: Incorrrect number of items iterated over. Count was %d\n", count);
        }
    }

    if (test_passed) {
        tests_passed += 1;
    }

    printf("Test Case 15: Remove the other half of the entries from the hashtable\n");
    tests_attempted += 1;
    test_passed = true;

    for (int i=data_len-rejected-1; i>=0 ;i--) {
        if (i%2==1) {
            rc = dragon_hashtable_remove(&tblHandle, (char*)&data[i]);
            if (rc != DRAGON_SUCCESS) {
                test_passed = false;
                printf("Error Code on remove was %u\n",rc);
            }
        }
    }

    rc = dragon_hashtable_stats(&tblHandle, &stats);

    if (rc != DRAGON_SUCCESS) {
        test_passed = false;
        printf("Error Code on remove was %u\n",rc);
    }

    if (stats.avg_chain_length != 0) {
        printf("Error: average chain length should be zero. It is %f\n", stats.avg_chain_length);
        test_passed = false;
    }

    if (test_passed) {
        tests_passed += 1;
    }

    rc = dragon_hashtable_dump("Final Hashtable State Dump\n=================================", &tblHandle, "");

    printf("Test Case 16: Test the destroy\n");
    tests_attempted = tests_attempted + 1;

    rc = dragon_hashtable_destroy(&tblHandle);

    if (rc != DRAGON_SUCCESS) {
        printf("Error Code was %u\n",rc);
    } else {
        tests_passed += 1;
    }

    free(hashtable_ptr);

    printf("Test Case 17: Test size function\n");
    tests_attempted = tests_attempted + 1;
    test_passed = false;

    rc = dragon_hashtable_size(600, sizeof(uint64_t)*2, sizeof(uint64_t)*2, &size);

    if (rc != DRAGON_SUCCESS) {
        printf("Error Code was %u\n",rc);
    } else {
        tests_passed += 1;
    }

    hashtable_ptr = malloc(size);

    printf("Test Case 18: Test init function\n");
    tests_attempted = tests_attempted + 1;
    test_passed = false;

    rc = dragon_hashtable_init(hashtable_ptr, &tblHandle, 600, sizeof(uint64_t)*2, sizeof(uint64_t)*2);

    if (rc != DRAGON_SUCCESS) {
        printf("Error Code was %u\n",rc);
    } else {
        tests_passed += 1;
    }

    printf("Test Case 19: Test add of large value in key\n");
    tests_attempted = tests_attempted + 1;
    test_passed = false;
    dragonMemoryManifestRec_t rec;

    rec.alloc_type = 1;
    rec.alloc_type_id = 999;
    rec.offset = 42;
    rec.size = 65;

    rc = dragon_hashtable_add(&tblHandle, (char*)&rec.alloc_type,(char*)&rec.offset);

    if (rc != DRAGON_SUCCESS) {
        printf("Error Code was %u\n",rc);
        test_passed = false;
    } else {
        tests_passed += 1;
    }

    printf("Test Case 20: Lookup entry in the hashtable\n");
    tests_attempted = tests_attempted + 1;
    test_passed = false;


    rec.alloc_type = 1;
    rec.alloc_type_id = 999;
    rec.offset = 0;
    rec.size = 0;

    rc = dragon_hashtable_get(&tblHandle, (char*)&rec.alloc_type,(char*)&rec.offset);
    if (rc == DRAGON_HASHTABLE_KEY_NOT_FOUND) {
        printf("Could not find key in hashtable.\n");
        test_passed = false;
    } else if (rc == DRAGON_SUCCESS) {
        if (rec.offset != 42 || rec.size != 65) {
            printf("There was a problem looking up key in the hashtable. The returned value was not correct.\n");
            test_passed = false;
        } else {
            tests_passed += 1;
        }
    }

    printf("Test Case 21: Add %lu entries to the hashtable\n", data_len);
    tests_attempted = tests_attempted + 1;
    test_passed = true;

    rejected = 0;

    for (int i=0;i<data_len;i++) {
        rec.alloc_type = data[i];
        rec.alloc_type_id = data[i]+1;
        rec.offset = data[i]+2;
        rec.size = data[i]+3;

        rc = dragon_hashtable_add(&tblHandle, (char*)&rec.alloc_type,(char*)&rec.offset);

        if (rc == DRAGON_HASHTABLE_FULL) {
            rejected = rejected + 1;
        } else if (rc != DRAGON_SUCCESS) {
            printf("Error Code was %u\n",rc);
            test_passed = false;
        }
    }

    if (test_passed && rejected == 0) {
        tests_passed += 1;
    } else {
        printf("There were %lu rejected adds\n", rejected);
    }


    printf("Test Case 22: Lookup %lu entries in the hashtable\n", data_len);
    tests_attempted = tests_attempted + 1;
    test_passed = true;

    rejected = 0;

    for (int i=0;i<data_len;i++) {
        rec.alloc_type = data[i];
        rec.alloc_type_id = data[i]+1;
        rec.offset = 0;
        rec.size = 0;
        rc = dragon_hashtable_get(&tblHandle, (char*)&rec.alloc_type,(char*)&rec.offset);
        if (rc == DRAGON_HASHTABLE_KEY_NOT_FOUND) {
            rejected = rejected + 1;
        } else if (rc == DRAGON_SUCCESS) {
            if (rec.offset != data[i]+2 || rec.size != data[i]+3) {
                printf("There was a problem looking up key %lu in the hashtable. Offset should have been %lu and it was %lu instead.\n", data[i], data[i]+2, rec.offset);
                printf("Or there was a problem looking up key %lu in the hashtable. Size should have been %lu and it was %lu instead.\n", data[i], data[i]+3, rec.size);
                test_passed = false;
            }
        } else if (rc != DRAGON_SUCCESS) {
            printf("Error Code was %u\n",rc);
        }
    }

    if (test_passed && rejected == 0) {
        tests_passed += 1;
    } else {
        printf("There were %lu rejected lookups of key not found\n", rejected);
    }

    printf("Test Case 23: Delete even indexed %lu entries in the hashtable\n", data_len);
    tests_attempted = tests_attempted + 1;
    test_passed = true;

    rejected = 0;

    for (int i=0;i<data_len;i+=2) {
        rec.alloc_type = data[i];
        rec.alloc_type_id = data[i]+1;
        rec.offset = 0;
        rec.size = 0;
        rc = dragon_hashtable_remove(&tblHandle, (char*)&rec.alloc_type);
        if (rc != DRAGON_SUCCESS) {
            rejected = rejected + 1;
            printf("Could not remove. Error Code was %u\n",rc);
        }
    }

    if (test_passed && rejected == 0) {
        tests_passed += 1;
    } else {
        printf("There were %lu rejected removes of keys\n", rejected);
    }

    printf("Test Case 24: Add even entries back into the hashtable\n");
    tests_attempted = tests_attempted + 1;
    test_passed = true;

    rejected = 0;

    for (int i=0;i<data_len;i+=2) {
        rec.alloc_type = data[i];
        rec.alloc_type_id = data[i]+1;
        rec.offset = data[i]+2;
        rec.size = data[i]+3;

        rc = dragon_hashtable_add(&tblHandle, (char*)&rec.alloc_type,(char*)&rec.offset);

        if (rc == DRAGON_HASHTABLE_FULL) {
            rejected = rejected + 1;
        } else if (rc != DRAGON_SUCCESS) {
            printf("Error Code was %u\n",rc);
            test_passed = false;
        }
    }

    if (test_passed && rejected == 0) {
        tests_passed += 1;
    } else {
        printf("There were %lu rejected adds\n", rejected);
    }

    printf("Test Case 25: Lookup odd index %lu entries in the hashtable\n", data_len);
    tests_attempted = tests_attempted + 1;
    test_passed = true;

    rejected = 0;

    for (int i=1;i<data_len;i+=2) {
        rec.alloc_type = data[i];
        rec.alloc_type_id = data[i]+1;
        rec.offset = 0;
        rec.size = 0;
        rc = dragon_hashtable_get(&tblHandle, (char*)&rec.alloc_type,(char*)&rec.offset);
        if (rc == DRAGON_HASHTABLE_KEY_NOT_FOUND) {
            rejected = rejected + 1;
        } else if (rc == DRAGON_SUCCESS) {
            if (rec.offset != data[i]+2 || rec.size != data[i]+3) {
                printf("There was a problem looking up key %lu in the hashtable. Offset should have been %lu and it was %lu instead.\n", data[i], data[i]+2, rec.offset);
                printf("Or there was a problem looking up key %lu in the hashtable. Size should have been %lu and it was %lu instead.\n", data[i], data[i]+3, rec.size);
                test_passed = false;
            }
        } else if (rc != DRAGON_SUCCESS) {
            printf("Error Code was %u\n",rc);
        }
    }

    if (test_passed && rejected == 0) {
        tests_passed += 1;
    } else {
        printf("There were %lu rejected lookups of key not found\n", rejected);
    }

    printf("Test Case 26: Remove value from end of a chain.\n");
    tests_attempted = tests_attempted + 1;

    rec.alloc_type = 9000;
    rec.alloc_type_id = 9001;
    rec.offset = 0;
    rec.size = 0;
    rc = dragon_hashtable_remove(&tblHandle, (char*)&rec.alloc_type);
    if (rc != DRAGON_SUCCESS) {
        printf("Could not remove from end of chain. Error Code was %u\n",rc);
    } else
        tests_passed += 1;

    printf("Test Case 27: Add to end of chain again.\n");
    tests_attempted = tests_attempted + 1;

    rec.alloc_type = 9000;
    rec.alloc_type_id = 9001;
    rec.offset = 42;
    rec.size = 24;

    rc = dragon_hashtable_add(&tblHandle, (char*)&rec.alloc_type,(char*)&rec.offset);

    if (rc != DRAGON_SUCCESS) {
        printf("Error Code was %u\n",rc);
    } else
        tests_passed += 1;

    printf("Test Case 28: Delete in middle and end of chain.\n");
    tests_attempted = tests_attempted + 1;

    rec.alloc_type = 9000;
    rec.alloc_type_id = 9001;
    rec.offset = 0;
    rec.size = 0;
    rc = dragon_hashtable_remove(&tblHandle, (char*)&rec.alloc_type);
    if (rc != DRAGON_SUCCESS) {
        printf("Could not remove from end of chain. Error Code was %u\n",rc);
    }

    rec.alloc_type = 54882;
    rec.alloc_type_id = 54883;
    rec.offset = 0;
    rec.size = 0;
    rc = dragon_hashtable_remove(&tblHandle, (char*)&rec.alloc_type);
    if (rc != DRAGON_SUCCESS) {
        printf("Could not remove from end of chain. Error Code was %u\n",rc);
    }

    rec.alloc_type = 9000;
    rec.alloc_type_id = 9001;
    rec.offset = 42;
    rec.size = 24;

    rc = dragon_hashtable_add(&tblHandle, (char*)&rec.alloc_type,(char*)&rec.offset);

    if (rc != DRAGON_SUCCESS) {
        printf("Error Code was %u\n",rc);
    } else
        tests_passed += 1;

    printf("Test Case 29: Delete from middle to end of chain.\n");
    tests_attempted = tests_attempted + 1;

    rec.alloc_type = 9000;
    rec.alloc_type_id = 9001;
    rec.offset = 0;
    rec.size = 0;
    rc = dragon_hashtable_remove(&tblHandle, (char*)&rec.alloc_type);
    if (rc != DRAGON_SUCCESS) {
        printf("Could not remove from end of chain. Error Code was %u\n",rc);
    }

    rec.alloc_type = 60463;
    rec.alloc_type_id = 60464;
    rec.offset = 0;
    rec.size = 0;
    rc = dragon_hashtable_remove(&tblHandle, (char*)&rec.alloc_type);
    if (rc != DRAGON_SUCCESS) {
        printf("Could not remove from end of chain. Error Code was %u\n",rc);
    }

    rec.alloc_type = 5459;
    rec.alloc_type_id = 5460;
    rec.offset = 0;
    rec.size = 0;
    rc = dragon_hashtable_remove(&tblHandle, (char*)&rec.alloc_type);
    if (rc != DRAGON_SUCCESS) {
        printf("Could not remove from end of chain. Error Code was %u\n",rc);
    }

    rec.alloc_type = 97746;
    rec.alloc_type_id = 97747;
    rec.offset = 0;
    rec.size = 0;
    rc = dragon_hashtable_remove(&tblHandle, (char*)&rec.alloc_type);
    if (rc != DRAGON_SUCCESS) {
        printf("Could not remove from end of chain. Error Code was %u\n",rc);
    }

    rec.alloc_type = 36067;
    rec.alloc_type_id = 36068;
    rec.offset = 0;
    rec.size = 0;
    rc = dragon_hashtable_remove(&tblHandle, (char*)&rec.alloc_type);
    if (rc != DRAGON_SUCCESS) {
        printf("Could not remove from end of chain. Error Code was %u\n",rc);
    } else
        tests_passed += 1;

    //rc = dragon_hashtable_dump("Final Hashtable State Dump\n=================================", &tblHandle, "");

    printf("%lu of %lu hashtable tests passed.\n", tests_passed, tests_attempted);

    if (tests_passed != tests_attempted)
        return FAILED;
    else
        return SUCCESS;
}

//
//  two_sum.c
//  leet-code-2021
//
//  Created by Sergey Kozachenko on 14.02.2021.
//

#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>

// compile time const
#define SIZE 50000

/**
 Fixes negative keys
 */
static int hash(int key) {
    int r = key % SIZE;
    return r < 0 ? r + SIZE : r;
}

static void insert(int *keys, int *values, int key, int value) {
    int index = hash(key);
    while (values[index]) {
        index = (index + 1) % SIZE;
    }
    keys[index] = key;
    values[index] = value;
}

static int search(int *keys, int *values, int key) {
    int index = hash(key);
    while (values[index]) {
        if (keys[index] == key) {
            return values[index];
        }
        index = (index + 1) % SIZE;
    }
    return 0;
}

/**
 Given an array of integers nums and an integer target, return indices of the two numbers such that they add up to target.

 You may assume that each input would have exactly one solution, and you may not use the same element twice.

 You can return the answer in any order.
 */
static int* twoSum(int* nums, int numsSize, int target, int* returnSize) {
    // Init
    *returnSize = 2;
    int *result = (int*)malloc(*returnSize * sizeof(int));
    
    // Bruteforce (optimized)
    /*
    int i, j;
    bool finished = false;
    
    for (i = 0; i < numsSize; i++) {
        if (finished) {
            break;
        } else {
            for (j = i + 1; j < numsSize; j++) {
                if (nums[i] + nums[j] == target) {
                    result[0] = i;
                    result[1] = j;
                    finished = true;
                    break;
                }
            }
        }
    }
    */
    
    // Hashtable for look back
    int keys[SIZE];
    int values[SIZE] = {0};
    for (int i = 0; i < numsSize; i++) {
        int complements = target - nums[i];
        int value = search(keys, values, complements);
        if (value) {
            result[0] = value - 1;
            result[1] = i;
            return result;
        }
        insert(keys, values, nums[i], i + 1);
    }
    
    
    return result;
}


static void twoSumEx1() {
    int s = 5000;
    int r = -7 % s;
    int v = r < 0 ? r + s : r;
    printf("s=%d, r=%d, v=%d\n", s,r,v);
    
    
    // example 1
    // Input: nums = [2,7,11,15], target = 9
    // Output: [0,1]
    // Output: Because nums[0] + nums[1] == 9, we return [0, 1].
    int nums[4] = {2, 7, 11, 15};
    int numsSize = 4, target = 9, returnSize;
    int *res = twoSum(nums, numsSize, target, &returnSize);
    printf("Example 1: res=[%d, %d]\n", res[0], res[1]);
}

static void twoSumEx2() {
    // example 2
    // Input: nums = [3,2,4], target = 6
    // Output: [1,2]
    int nums[3] = {3, 2, 4};
    int numsSize = 3, target = 6, returnSize;
    int *res = twoSum(nums, numsSize, target, &returnSize);
    printf("Example 2: res=[%d, %d]\n", res[0], res[1]);
}

static void twoSumEx3() {
    // example 3
    // Input: nums = [3,3], target = 6
    // Output: [0,1]
    int nums[2] = {3, 3};
    int numsSize = 2, target = 6, returnSize;
    int *res = twoSum(nums, numsSize, target, &returnSize);
    printf("Example 3: res=[%d, %d]\n", res[0], res[1]);
}

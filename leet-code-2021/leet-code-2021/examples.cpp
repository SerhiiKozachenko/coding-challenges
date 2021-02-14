//
//  examples.cpp
//  leet-code-2021
//
//  Created by Sergey Kozachenko on 14.02.2021.
//

#include <iostream>
#include <vector>
#include <array>

using namespace std;

namespace examples {


// Classes
class Solution {
public:
    vector<int> method1(vector<int>& nums, int target) {
        std::cout << "method1\n";
        vector<int> res;
        res.push_back(10);
        
        return res;
    }
};

/**
 * Note: The returned array must be malloced, assume caller calls free().
 * https://www.tutorialspoint.com/cprogramming/c_return_arrays_from_function.htm
 *
 * pointers * vs &
 * https://stackoverflow.com/questions/2094666/pointers-in-c-when-to-use-the-ampersand-and-the-asterisk
 * https://stackoverflow.com/questions/18041100/using-c-string-gives-warning-address-of-stack-memory-associated-with-local-var
 *
 * https://www.techiedelight.com/rearrange-the-array-with-alternate-high-and-low-elements/
 *
 * https://www.geeksforgeeks.org/dynamic-memory-allocation-in-c-using-malloc-calloc-free-and-realloc/
 * Since the size of int is 4 bytes, this statement will allocate 400 bytes of memory.
 * And, the pointer ptr holds the address of the first byte in the allocated memory.
 * ptr = (int*) malloc(100 * sizeof(int));
 * https://www.tutorialspoint.com/c_standard_library/c_function_malloc.htm
 */

    // Including duplicate errors
    // https://chunminchang.github.io/blog/post/how-to-avoid-duplicate-symbols-when-compiling

    // Why header files needed
    // https://stackoverflow.com/questions/1686204/why-should-i-not-include-cpp-files-and-instead-use-a-header

    static void main() {
        std::cout << "Examples:\n";
        
        // Create instance of class
        // https://stackoverflow.com/questions/12248703/creating-an-instance-of-class
        Solution sol = Solution();
        std::vector<int> nums;
        nums.push_back(10);
        sol.method1(nums, 2);
        
        // Diff vector<int> vs int[] vs int*
        // https://www.educba.com/c-plus-plus-vector-vs-array/
        // https://stackoverflow.com/questions/11555997/c-c-int-vs-int-pointers-vs-array-notation-what-is-the-difference
        
        // Vectors are dynamic sized
        std::vector<int> vec1 { 1, 2, 3, 4 };
        int v1 = vec1[0];
        int v3 = vec1[3];
        vec1.push_back(5);
        int v4 = vec1[4];
        std::cout << "vec1 size:" << vec1.size() << "\n";
        vec1.pop_back();
        std::cout << "vec1 size after pop_back:" << vec1.size() << "\n";
        
        // Arrays are low level static sized
        int arr[] = {0, 1, 2, 3, 4};
        int a1 = arr[1];
        int a3 = arr[3];
        // Can't push new element to array, only create new array with bigger size
        // Can't determine size of array easily when passed to other fn's, unless passed within a struct
        std::cout << "arr[] size:" << sizeof(arr) / sizeof(arr[0]) << "\n";
        
        // STL  - standard template library <array>
        // https://www.tutorialspoint.com/difference-between-std-vector-and-std-array-in-cplusplus
    }
}


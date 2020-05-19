﻿using System;
using System.Collections.Generic;
using System.Text;

namespace TrimDB.Core.Facts
{
    internal class CommonData
    {
        public static string[] Words => System.IO.File.ReadAllLines("words.txt");

        public static byte[] SingleBlock = new byte[] { 2, 0, 0, 0, 38, 99, 8, 0, 0, 0, 86, 65, 76, 85, 69, 61, 38, 99, 2, 0, 0, 0, 39, 100, 8, 0, 0, 0, 86, 65, 76, 85, 69, 61, 39, 100, 3, 0, 0, 0, 39, 101, 109, 9, 0, 0, 0, 86, 65, 76, 85, 69, 61, 39, 101, 109, 3, 0, 0, 0, 39, 108, 108, 9, 0, 0, 0, 86, 65, 76, 85, 69, 61, 39, 108, 108, 2, 0, 0, 0, 39, 109, 8, 0, 0, 0, 86, 65, 76, 85, 69, 61, 39, 109, 4, 0, 0, 0, 39, 109, 105, 100, 10, 0, 0, 0, 86, 65, 76, 85, 69, 61, 39, 109, 105, 100, 6, 0, 0, 0, 39, 109, 105, 100, 115, 116, 12, 0, 0, 0, 86, 65, 76, 85, 69, 61, 39, 109, 105, 100, 115, 116, 7, 0, 0, 0, 39, 109, 111, 110, 103, 115, 116, 13, 0, 0, 0, 86, 65, 76, 85, 69, 61, 39, 109, 111, 110, 103, 115, 116, 9, 0, 0, 0, 39, 112, 114, 101, 110, 116, 105, 99, 101, 15, 0, 0, 0, 86, 65, 76, 85, 69, 61, 39, 112, 114, 101, 110, 116, 105, 99, 101, 3, 0, 0, 0, 39, 114, 101, 9, 0, 0, 0, 86, 65, 76, 85, 69, 61, 39, 114, 101, 2, 0, 0, 0, 39, 115, 8, 0, 0, 0, 86, 65, 76, 85, 69, 61, 39, 115, 7, 0, 0, 0, 39, 115, 98, 108, 111, 111, 100, 13, 0, 0, 0, 86, 65, 76, 85, 69, 61, 39, 115, 98, 108, 111, 111, 100, 10, 0, 0, 0, 39, 115, 98, 111, 100, 105, 107, 105, 110, 115, 16, 0, 0, 0, 86, 65, 76, 85, 69, 61, 39, 115, 98, 111, 100, 105, 107, 105, 110, 115, 7, 0, 0, 0, 39, 115, 100, 101, 97, 116, 104, 13, 0, 0, 0, 86, 65, 76, 85, 69, 61, 39, 115, 100, 101, 97, 116, 104, 6, 0, 0, 0, 39, 115, 102, 111, 111, 116, 12, 0, 0, 0, 86, 65, 76, 85, 69, 61, 39, 115, 102, 111, 111, 116, 7, 0, 0, 0, 39, 115, 104, 101, 97, 114, 116, 13, 0, 0, 0, 86, 65, 76, 85, 69, 61, 39, 115, 104, 101, 97, 114, 116, 5, 0, 0, 0, 39, 115, 104, 117, 110, 11, 0, 0, 0, 86, 65, 76, 85, 69, 61, 39, 115, 104, 117, 110, 5, 0, 0, 0, 39, 115, 108, 105, 100, 11, 0, 0, 0, 86, 65, 76, 85, 69, 61, 39, 115, 108, 105, 100, 6, 0, 0, 0, 39, 115, 108, 105, 102, 101, 12, 0, 0, 0, 86, 65, 76, 85, 69, 61, 39, 115, 108, 105, 102, 101, 7, 0, 0, 0, 39, 115, 108, 105, 103, 104, 116, 13, 0, 0, 0, 86, 65, 76, 85, 69, 61, 39, 115, 108, 105, 103, 104, 116, 7, 0, 0, 0, 39, 115, 110, 97, 105, 108, 115, 13, 0, 0, 0, 86, 65, 76, 85, 69, 61, 39, 115, 110, 97, 105, 108, 115, 8, 0, 0, 0, 39, 115, 116, 114, 101, 119, 116, 104, 14, 0, 0, 0, 86, 65, 76, 85, 69, 61, 39, 115, 116, 114, 101, 119, 116, 104, 2, 0, 0, 0, 39, 116, 8, 0, 0, 0, 86, 65, 76, 85, 69, 61, 39, 116, 4, 0, 0, 0, 39, 116, 105, 108, 10, 0, 0, 0, 86, 65, 76, 85, 69, 61, 39, 116, 105, 108, 4, 0, 0, 0, 39, 116, 105, 115, 10, 0, 0, 0, 86, 65, 76, 85, 69, 61, 39, 116, 105, 115, 5, 0, 0, 0, 39, 116, 119, 97, 115, 11, 0, 0, 0, 86, 65, 76, 85, 69, 61, 39, 116, 119, 97, 115, 6, 0, 0, 0, 39, 116, 119, 101, 101, 110, 12, 0, 0, 0, 86, 65, 76, 85, 69, 61, 39, 116, 119, 101, 101, 110, 12, 0, 0, 0, 39, 116, 119, 101, 101, 110, 45, 100, 101, 99, 107, 115, 18, 0, 0, 0, 86, 65, 76, 85, 69, 61, 39, 116, 119, 101, 101, 110, 45, 100, 101, 99, 107, 115, 6, 0, 0, 0, 39, 116, 119, 101, 114, 101, 12, 0, 0, 0, 86, 65, 76, 85, 69, 61, 39, 116, 119, 101, 114, 101, 6, 0, 0, 0, 39, 116, 119, 105, 108, 108, 12, 0, 0, 0, 86, 65, 76, 85, 69, 61, 39, 116, 119, 105, 108, 108, 6, 0, 0, 0, 39, 116, 119, 105, 120, 116, 12, 0, 0, 0, 86, 65, 76, 85, 69, 61, 39, 116, 119, 105, 120, 116, 7, 0, 0, 0, 39, 116, 119, 111, 117, 108, 100, 13, 0, 0, 0, 86, 65, 76, 85, 69, 61, 39, 116, 119, 111, 117, 108, 100, 3, 0, 0, 0, 39, 117, 110, 9, 0, 0, 0, 86, 65, 76, 85, 69, 61, 39, 117, 110, 3, 0, 0, 0, 39, 118, 101, 9, 0, 0, 0, 86, 65, 76, 85, 69, 61, 39, 118, 101, 3, 0, 0, 0, 45, 105, 45, 9, 0, 0, 0, 86, 65, 76, 85, 69, 61, 45, 105, 45, 4, 0, 0, 0, 45, 110, 39, 116, 10, 0, 0, 0, 86, 65, 76, 85, 69, 61, 45, 110, 39, 116, 3, 0, 0, 0, 45, 111, 45, 9, 0, 0, 0, 86, 65, 76, 85, 69, 61, 45, 111, 45, 3, 0, 0, 0, 45, 115, 39, 9, 0, 0, 0, 86, 65, 76, 85, 69, 61, 45, 115, 39, 8, 0, 0, 0, 49, 48, 45, 112, 111, 105, 110, 116, 14, 0, 0, 0, 86, 65, 76, 85, 69, 61, 49, 48, 45, 112, 111, 105, 110, 116, 4, 0, 0, 0, 49, 48, 56, 48, 10, 0, 0, 0, 86, 65, 76, 85, 69, 61, 49, 48, 56, 48, 4, 0, 0, 0, 49, 48, 116, 104, 10, 0, 0, 0, 86, 65, 76, 85, 69, 61, 49, 48, 116, 104, 8, 0, 0, 0, 49, 49, 45, 112, 111, 105, 110, 116, 14, 0, 0, 0, 86, 65, 76, 85, 69, 61, 49, 49, 45, 112, 111, 105, 110, 116, 8, 0, 0, 0, 49, 50, 45, 112, 111, 105, 110, 116, 14, 0, 0, 0, 86, 65, 76, 85, 69, 61, 49, 50, 45, 112, 111, 105, 110, 116, 8, 0, 0, 0, 49, 54, 45, 112, 111, 105, 110, 116, 14, 0, 0, 0, 86, 65, 76, 85, 69, 61, 49, 54, 45, 112, 111, 105, 110, 116, 8, 0, 0, 0, 49, 56, 45, 112, 111, 105, 110, 116, 14, 0, 0, 0, 86, 65, 76, 85, 69, 61, 49, 56, 45, 112, 111, 105, 110, 116, 3, 0, 0, 0, 49, 115, 116, 9, 0, 0, 0, 86, 65, 76, 85, 69, 61, 49, 115, 116, 1, 0, 0, 0, 50, 7, 0, 0, 0, 86, 65, 76, 85, 69, 61, 50, 7, 0, 0, 0, 50, 44, 52, 44, 53, 45, 116, 13, 0, 0, 0, 86, 65, 76, 85, 69, 61, 50, 44, 52, 44, 53, 45, 116, 5, 0, 0, 0, 50, 44, 52, 45, 100, 11, 0, 0, 0, 86, 65, 76, 85, 69, 61, 50, 44, 52, 45, 100, 8, 0, 0, 0, 50, 48, 45, 112, 111, 105, 110, 116, 14, 0, 0, 0, 86, 65, 76, 85, 69, 61, 50, 48, 45, 112, 111, 105, 110, 116, 2, 0, 0, 0, 50, 68, 8, 0, 0, 0, 86, 65, 76, 85, 69, 61, 50, 68, 3, 0, 0, 0, 50, 110, 100, 9, 0, 0, 0, 86, 65, 76, 85, 69, 61, 50, 110, 100, 3, 0, 0, 0, 51, 45, 68, 9, 0, 0, 0, 86, 65, 76, 85, 69, 61, 51, 45, 68, 5, 0, 0, 0, 51, 48, 45, 51, 48, 11, 0, 0, 0, 86, 65, 76, 85, 69, 61, 51, 48, 45, 51, 48, 2, 0, 0, 0, 51, 68, 8, 0, 0, 0, 86, 65, 76, 85, 69, 61, 51, 68, 2, 0, 0, 0, 51, 77, 8, 0, 0, 0, 86, 65, 76, 85, 69, 61, 51, 77, 3, 0, 0, 0, 51, 114, 100, 9, 0, 0, 0, 86, 65, 76, 85, 69, 61, 51, 114, 100, 3, 0, 0, 0, 52, 45, 68, 9, 0, 0, 0, 86, 65, 76, 85, 69, 61, 52, 45, 68, 8, 0, 0, 0, 52, 56, 45, 112, 111, 105, 110, 116, 14, 0, 0, 0, 86, 65, 76, 85, 69, 61, 52, 56, 45, 112, 111, 105, 110, 116, 3, 0, 0, 0, 52, 71, 76, 9, 0, 0, 0, 86, 65, 76, 85, 69, 61, 52, 71, 76, 2, 0, 0, 0, 52, 72, 8, 0, 0, 0, 86, 65, 76, 85, 69, 61, 52, 72, 3, 0, 0, 0, 52, 116, 104, 9, 0, 0, 0, 86, 65, 76, 85, 69, 61, 52, 116, 104, 3, 0, 0, 0, 53, 45, 84, 9, 0, 0, 0, 86, 65, 76, 85, 69, 61, 53, 45, 84, 7, 0, 0, 0, 53, 45, 112, 111, 105, 110, 116, 13, 0, 0, 0, 86, 65, 76, 85, 69, 61, 53, 45, 112, 111, 105, 110, 116, 3, 0, 0, 0, 53, 116, 104, 9, 0, 0, 0, 86, 65, 76, 85, 69, 61, 53, 116, 104, 7, 0, 0, 0, 54, 45, 112, 111, 105, 110, 116, 13, 0, 0, 0, 86, 65, 76, 85, 69, 61, 54, 45, 112, 111, 105, 110, 116, 3, 0, 0, 0, 54, 116, 104, 9, 0, 0, 0, 86, 65, 76, 85, 69, 61, 54, 116, 104, 7, 0, 0, 0, 55, 45, 112, 111, 105, 110, 116, 13, 0, 0, 0, 86, 65, 76, 85, 69, 61, 55, 45, 112, 111, 105, 110, 116, 3, 0, 0, 0, 55, 116, 104, 9, 0, 0, 0, 86, 65, 76, 85, 69, 61, 55, 116, 104, 7, 0, 0, 0, 56, 45, 112, 111, 105, 110, 116, 13, 0, 0, 0, 86, 65, 76, 85, 69, 61, 56, 45, 112, 111, 105, 110, 116, 3, 0, 0, 0, 56, 116, 104, 9, 0, 0, 0, 86, 65, 76, 85, 69, 61, 56, 116, 104, 7, 0, 0, 0, 57, 45, 112, 111, 105, 110, 116, 13, 0, 0, 0, 86, 65, 76, 85, 69, 61, 57, 45, 112, 111, 105, 110, 116, 3, 0, 0, 0, 57, 116, 104, 9, 0, 0, 0, 86, 65, 76, 85, 69, 61, 57, 116, 104, 3, 0, 0, 0, 65, 38, 77, 9, 0, 0, 0, 86, 65, 76, 85, 69, 61, 65, 38, 77, 3, 0, 0, 0, 65, 38, 80, 9, 0, 0, 0, 86, 65, 76, 85, 69, 61, 65, 38, 80, 6, 0, 0, 0, 65, 39, 97, 115, 105, 97, 12, 0, 0, 0, 86, 65, 76, 85, 69, 61, 65, 39, 97, 115, 105, 97, 3, 0, 0, 0, 65, 45, 49, 9, 0, 0, 0, 86, 65, 76, 85, 69, 61, 65, 45, 49, 4, 0, 0, 0, 65, 45, 79, 75, 10, 0, 0, 0, 86, 65, 76, 85, 69, 61, 65, 45, 79, 75, 7, 0, 0, 0, 65, 45, 97, 110, 100, 45, 82, 13, 0, 0, 0, 86, 65, 76, 85, 69, 61, 65, 45, 97, 110, 100, 45, 82, 6, 0, 0, 0, 65, 45, 97, 120, 101, 115, 12, 0, 0, 0, 86, 65, 76, 85, 69, 61, 65, 45, 97, 120, 101, 115, 6, 0, 0, 0, 65, 45, 97, 120, 105, 115, 12, 0, 0, 0, 86, 65, 76, 85, 69, 61, 65, 45, 97, 120, 105, 115, 7, 0, 0, 0, 65, 45, 98, 108, 97, 115, 116, 13, 0, 0, 0, 86, 65, 76, 85, 69, 61, 65, 45, 98, 108, 97, 115, 116, 6, 0, 0, 0, 65, 45, 98, 111, 109, 98, 12, 0, 0, 0, 86, 65, 76, 85, 69, 61, 65, 45, 98, 111, 109, 98, 5, 0, 0, 0, 65, 45, 100, 97, 121, 11, 0, 0, 0, 86, 65, 76, 85, 69, 61, 65, 45, 100, 97, 121, 6, 0, 0, 0, 65, 45, 102, 108, 97, 116, 12, 0, 0, 0, 86, 65, 76, 85, 69, 61, 65, 45, 102, 108, 97, 116, 7, 0, 0, 0, 65, 45, 102, 114, 97, 109, 101, 13, 0, 0, 0, 86, 65, 76, 85, 69, 61, 65, 45, 102, 114, 97, 109, 101, 6, 0, 0, 0, 65, 45, 108, 105, 110, 101, 12, 0, 0, 0, 86, 65, 76, 85, 69, 61, 65, 45, 108, 105, 110, 101, 5, 0, 0, 0, 65, 45, 111, 110, 101, 11, 0, 0, 0, 86, 65, 76, 85, 69, 61, 65, 45, 111, 110, 101, 6, 0, 0, 0, 65, 45, 112, 111, 108, 101, 12, 0, 0, 0, 86, 65, 76, 85, 69, 61, 65, 45, 112, 111, 108, 101, 7, 0, 0, 0, 65, 45, 115, 99, 111, 112, 101, 13, 0, 0, 0, 86, 65, 76, 85, 69, 61, 65, 45, 115, 99, 111, 112, 101, 8, 0, 0, 0, 65, 45, 115, 104, 97, 112, 101, 100, 14, 0, 0, 0, 86, 65, 76, 85, 69, 61, 65, 45, 115, 104, 97, 112, 101, 100, 7, 0, 0, 0, 65, 45, 115, 104, 97, 114, 112, 13, 0, 0, 0, 86, 65, 76, 85, 69, 61, 65, 45, 115, 104, 97, 114, 112, 6, 0, 0, 0, 65, 45, 116, 101, 110, 116, 12, 0, 0, 0, 86, 65, 76, 85, 69, 61, 65, 45, 116, 101, 110, 116, 5, 0, 0, 0, 65, 45, 119, 97, 114, 11, 0, 0, 0, 86, 65, 76, 85, 69, 61, 65, 45, 119, 97, 114, 9, 0, 0, 0, 65, 45, 119, 101, 97, 112, 111, 110, 115, 15, 0, 0, 0, 86, 65, 76, 85, 69, 61, 65, 45, 119, 101, 97, 112, 111, 110, 115, 2, 0, 0, 0, 65, 46, 8, 0, 0, 0, 86, 65, 76, 85, 69, 61, 65, 46, 6, 0, 0, 0, 65, 46, 65, 46, 65, 46, 12, 0, 0, 0, 86, 65, 76, 85, 69, 61, 65, 46, 65, 46, 65, 46, 4, 0, 0, 0, 65, 46, 66, 46, 10, 0, 0, 0, 86, 65, 76, 85, 69, 61, 65, 46, 66, 46, 6, 0, 0, 0, 65, 46, 66, 46, 65, 46, 12, 0, 0, 0, 86, 65, 76, 85, 69, 61, 65, 46, 66, 46, 65, 46, 4, 0, 0, 0, 65, 46, 67, 46, 10, 0, 0, 0, 86, 65, 76, 85, 69, 61, 65, 46, 67, 46, 4, 0, 0, 0, 65, 46, 68, 46, 10, 0, 0, 0, 86, 65, 76, 85, 69, 61, 65, 46, 68, 46, 6, 0, 0, 0, 65, 46, 68, 46, 67, 46, 12, 0, 0, 0, 86, 65, 76, 85, 69, 61, 65, 46, 68, 46, 67, 46, 4, 0, 0, 0, 65, 46, 70, 46, 10, 0, 0, 0, 86, 65, 76, 85, 69, 61, 65, 46, 70, 46, 8, 0, 0, 0, 65, 46, 70, 46, 65, 46, 77, 46, 14, 0, 0, 0, 86, 65, 76, 85, 69, 61, 65, 46, 70, 46, 65, 46, 77, 46, 4, 0, 0, 0, 65, 46, 71, 46, 10, 0, 0, 0, 86, 65, 76, 85, 69, 61, 65, 46, 71, 46, 4, 0, 0, 0, 65, 46, 72, 46, 10, 0, 0, 0, 86, 65, 76, 85, 69, 61, 65, 46, 72, 46, 4, 0, 0, 0, 65, 46, 73, 46, 10, 0, 0, 0, 86, 65, 76, 85, 69, 61, 65, 46, 73, 46, 6, 0, 0, 0, 65, 46, 73, 46, 65, 46, 12, 0, 0, 0, 86, 65, 76, 85, 69, 61, 65, 46, 73, 46, 65, 46, 6, 0, 0, 0, 65, 46, 73, 46, 68, 46, 12, 0, 0, 0, 86, 65, 76, 85, 69, 61, 65, 46, 73, 46, 68, 46, 4, 0, 0, 0, 65, 46, 76, 46, 10, 0, 0, 0, 86, 65, 76, 85, 69, 61, 65, 46, 76, 46, 6, 0, 0, 0, 65, 46, 76, 46, 80, 46, 12, 0, 0, 0, 86, 65, 76, 85, 69, 61, 65, 46, 76, 46, 80, 46, 4, 0, 0, 0, 65, 46, 77, 46, 10, 0, 0, 0, 86, 65, 76, 85, 69, 61, 65, 46, 77, 46, 6, 0, 0, 0, 65, 46, 77, 46, 65, 46, 12, 0, 0, 0, 86, 65, 76, 85, 69, 61, 65, 46, 77, 46, 65, 46, 8, 0, 0, 0, 65, 46, 77, 46, 68, 46, 71, 46, 14, 0, 0, 0, 86, 65, 76, 85, 69, 61, 65, 46, 77, 46, 68, 46, 71, 46, 4, 0, 0, 0, 65, 46, 78, 46, 10, 0, 0, 0, 86, 65, 76, 85, 69, 61, 65, 46, 78, 46, 8, 0, 0, 0, 65, 46, 82, 46, 67, 46, 83, 46, 14, 0, 0, 0, 86, 65, 76, 85, 69, 61, 65, 46, 82, 46, 67, 46, 83, 46, 4, 0, 0, 0, 65, 46, 85, 46, 10, 0, 0, 0, 86, 65, 76, 85, 69, 61, 65, 46, 85, 46, 6, 0, 0, 0, 65, 46, 85, 46, 67, 46, 12, 0, 0, 0, 86, 65, 76, 85, 69, 61, 65, 46, 85, 46, 67, 46, 4, 0, 0, 0, 65, 46, 86, 46, 10, 0, 0, 0, 86, 65, 76, 85, 69, 61, 65, 46, 86, 46, 8, 0, 0, 0, 65, 46, 87, 46, 79, 46, 76, 46, 14, 0, 0, 0, 86, 65, 76, 85, 69, 61, 65, 46, 87, 46, 79, 46, 76, 46, 3, 0, 0, 0, 65, 47, 67, 9, 0, 0, 0, 86, 65, 76, 85, 69, 61, 65, 47, 67, 3, 0, 0, 0, 65, 47, 70, 9, 0, 0, 0, 86, 65, 76, 85, 69, 61, 65, 47, 70, 3, 0, 0, 0, 65, 47, 79, 9, 0, 0, 0, 86, 65, 76, 85, 69, 61, 65, 47, 79, 3, 0, 0, 0, 65, 47, 80, 9, 0, 0, 0, 86, 65, 76, 85, 69, 61, 65, 47, 80, 3, 0, 0, 0, 65, 47, 86, 9, 0, 0, 0, 86, 65, 76, 85, 69, 61, 65, 47, 86, 2, 0, 0, 0, 65, 49, 8, 0, 0, 0, 86, 65, 76, 85, 69, 61, 65, 49, 2, 0, 0, 0, 65, 52, 8, 0, 0, 0, 86, 65, 76, 85, 69, 61, 65, 52, 2, 0, 0, 0, 65, 53, 8, 0, 0, 0, 86, 65, 76, 85, 69, 61, 65, 53, 2, 0, 0, 0, 65, 65, 8, 0, 0, 0, 86, 65, 76, 85, 69, 61, 65, 65, 3, 0, 0, 0, 65, 65, 65, 9, 0, 0, 0, 86, 65, 76, 85, 69, 61, 65, 65, 65, 4, 0, 0, 0, 65, 65, 65, 65, 10, 0, 0, 0, 86, 65, 76, 85, 69, 61, 65, 65, 65, 65, 6, 0, 0, 0, 65, 65, 65, 65, 65, 65, 12, 0, 0, 0, 86, 65, 76, 85, 69, 61, 65, 65, 65, 65, 65, 65, 4, 0, 0, 0, 65, 65, 65, 76, 10, 0, 0, 0, 86, 65, 76, 85, 69, 61, 65, 65, 65, 76, 4, 0, 0, 0, 65, 65, 65, 83, 10, 0, 0, 0, 86, 65, 76, 85, 69, 61, 65, 65, 65, 83, 3, 0, 0, 0, 65, 65, 69, 9, 0, 0, 0, 86, 65, 76, 85, 69, 61, 65, 65, 69, 4, 0, 0, 0, 65, 65, 69, 69, 10, 0, 0, 0, 86, 65, 76, 85, 69, 61, 65, 65, 69, 69, 3, 0, 0, 0, 65, 65, 70, 9, 0, 0, 0, 86, 65, 76, 85, 69, 61, 65, 65, 70, 3, 0, 0, 0, 65, 65, 71, 9, 0, 0, 0, 86, 65, 76, 85, 69, 61, 65, 65, 71, 4, 0, 0, 0, 65, 65, 73, 73, 10, 0, 0, 0, 86, 65, 76, 85, 69, 61, 65, 65, 73, 73, 3, 0, 0, 0, 65, 65, 77, 9, 0, 0, 0, 86, 65, 76, 85, 69, 61, 65, 65, 77, 5, 0, 0, 0, 65, 65, 77, 83, 73, 11, 0, 0, 0, 86, 65, 76, 85, 69, 61, 65, 65, 77, 83, 73, 3, 0, 0, 0, 65, 65, 79, 9, 0, 0, 0, 86, 65, 76, 85, 69, 61, 65, 65, 79, 3, 0, 0, 0, 65, 65, 80, 9, 0, 0, 0, 86, 65, 76, 85, 69, 61, 65, 65, 80, 5, 0, 0, 0, 65, 65, 80, 83, 83, 11, 0, 0, 0, 86, 65, 76, 85, 69, 61, 65, 65, 80, 83, 83, 4, 0, 0, 0, 65, 65, 82, 67, 10, 0, 0, 0, 86, 65, 76, 85, 69, 61, 65, 65, 82, 67, 4, 0, 0, 0, 65, 65, 82, 80, 10, 0, 0, 0, 86, 65, 76, 85, 69, 61, 65, 65, 82, 80, 3, 0, 0, 0, 65, 65, 83, 9, 0, 0, 0, 86, 65, 76, 85, 69, 61, 65, 65, 83, 3, 0, 0, 0, 65, 65, 85, 9, 0, 0, 0, 86, 65, 76, 85, 69, 61, 65, 65, 85, 4, 0, 0, 0, 65, 65, 85, 80, 10, 0, 0, 0, 86, 65, 76, 85, 69, 61, 65, 65, 85, 80, 4, 0, 0, 0, 65, 65, 85, 87, 10, 0, 0, 0, 86, 65, 76, 85, 69, 61, 65, 65, 85, 87, 5, 0, 0, 0, 65, 65, 86, 83, 79, 11, 0, 0, 0, 86, 65, 76, 85, 69, 61, 65, 65, 86, 83, 79, 3, 0, 0, 0, 65, 65, 88, 9, 0, 0, 0, 86, 65, 76, 85, 69, 61, 65, 65, 88, 2, 0, 0, 0, 65, 66, 8, 0, 0, 0, 86, 65, 76, 85, 69, 61, 65, 66, 3, 0, 0, 0, 65, 66, 65, 9, 0, 0, 0, 86, 65, 76, 85, 69, 61, 65, 66, 65, 5, 0, 0, 0, 65, 66, 65, 84, 83, 11, 0, 0, 0, 86, 65, 76, 85, 69, 61, 65, 66, 65, 84, 83, 4, 0, 0, 0, 65, 66, 66, 82, 10, 0, 0, 0, 86, 65, 76, 85, 69, 61, 65, 66, 66, 82, 3, 0, 0, 0, 65, 66, 67, 9, 0, 0, 0, 86, 65, 76, 85, 69, 61, 65, 66, 67, 4, 0, 0, 0, 65, 66, 67, 115, 10, 0, 0, 0, 86, 65, 76, 85, 69, 61, 65, 66, 67, 115, 4, 0, 0, 0, 65, 66, 69, 76, 10, 0, 0, 0, 86, 65, 76, 85, 69, 61, 65, 66, 69, 76, 5, 0, 0, 0, 65, 66, 69, 80, 80, 11, 0, 0, 0, 86, 65, 76, 85, 69, 61, 65, 66, 69, 80, 80, 4, 0, 0, 0, 65, 66, 70, 77, 10, 0, 0, 0, 86, 65, 76, 85, 69, 61, 65, 66, 70, 77, 4, 0, 0, 0, 65, 66, 72, 67, 10, 0, 0, 0, 86, 65, 76, 85, 69, 61, 65, 66, 72, 67, 3, 0, 0, 0, 65, 66, 73, 9, 0, 0, 0, 86, 65, 76, 85, 69, 61, 65, 66, 73, 4, 0, 0, 0, 65, 66, 76, 83, 10, 0, 0, 0, 86, 65, 76, 85, 69, 61, 65, 66, 76, 83, 3, 0, 0, 0, 65, 66, 77, 9, 0, 0, 0, 86, 65, 76, 85, 69, 61, 65, 66, 77, 4, 0, 0, 0, 65, 66, 80, 67, 10, 0, 0, 0, 86, 65, 76, 85, 69, 61, 65, 66, 80, 67, 3, 0, 0, 0, 65, 66, 83, 9, 0, 0, 0, 86, 65, 76, 85, 69, 61, 65, 66, 83, 5, 0, 0, 0, 65, 66, 83, 66, 72, 11, 0, 0, 0, 86, 65, 76, 85, 69, 61, 65, 66, 83, 66, 72, 5, 0, 0, 0, 65, 67, 45, 68, 67, 11, 0, 0, 0, 86, 65, 76, 85, 69, 61, 65, 67, 45, 68, 67, 5, 0, 0, 0, 65, 67, 47, 68, 67, 11, 0, 0, 0, 86, 65, 76, 85, 69, 61, 65, 67, 47, 68, 67, 4, 0, 0, 0, 65, 67, 65, 65, 10, 0, 0, 0, 86, 65, 76, 85, 69, 61, 65, 67, 65, 65, 4, 0, 0, 0, 65, 67, 65, 83, 10, 0, 0, 0, 86, 65, 76, 85, 69, 61, 65, 67, 65, 83, 5, 0, 0, 0, 65, 67, 65, 87, 83, 11, 0, 0, 0, 86, 65, 76, 85, 69, 61, 65, 67, 65, 87, 83, 3, 0, 0, 0, 65, 67, 66, 9, 0, 0, 0, 86, 65, 76, 85, 69, 61, 65, 67, 66, 4, 0, 0, 0, 65, 67, 66, 76, 10, 0, 0, 0, 86, 65, 76, 85, 69, 61, 65, 67, 66, 76, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
    }
}

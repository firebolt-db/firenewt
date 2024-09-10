DROP TABLE IF EXISTS zbuck;
CREATE TABLE zbuck (bucket_index bigint NOT NULL, bucket_start bigint NOT NULL, bucket_end bigint NOT NULL);
INSERT INTO zbuck
SELECT   
   b_ind - 1 bucket_index,
   b_z bucket_start, 
   LEAD(b_z-1, 1, 9223372036854775807) OVER (ORDER BY b_ind) AS bucket_end
FROM
(SELECT b_z, b_ind
FROM 
(SELECT a.z, ARRAY_ENUMERATE(a.z) ind
FROM
	(SELECT [0, 8216, 14025, 18768, 22876, 26550, 29904, 33009, 35914, 38653, 41251, 43728, 46100, 48379, 50575, 52696, 54750, 56743, 58679, 60564, 62401, 64194, 65946, 67659, 69336, 70979, 72590, 74171, 75724, 77250, 78750, 80226, 81678, 83108, 84517, 85906, 87275, 88626, 89959, 91275, 92574, 93857, 95125, 96378, 97617, 98842, 100053, 101251, 102437, 103611, 104773, 105923, 107062, 108191, 109309, 110417, 111515, 112603, 113682, 114752, 115813, 116865, 117908, 118943, 119970, 120989, 122000, 123004, 124000, 124989, 125971, 126946, 127914, 128876, 129831, 130780, 131722, 132658, 133588, 134512, 135431, 136344, 137251, 138153, 139049, 139940, 140826, 141707, 142583, 143454, 144320, 145181, 146038, 146890, 147737, 148580, 149419, 150253, 151083, 151909, 152731, 153549, 154362, 155172, 155978, 156780, 157578, 158372, 159163, 159950, 160733, 161513, 162289, 163062, 163831, 164597, 165360, 166120, 166876, 167629, 168379, 169126, 169870, 170611, 171349, 172084, 172816, 173545, 174271, 174994, 175715, 176433, 177148, 177860, 178570, 179277, 179982, 180684, 181383, 182080, 182774, 183466, 184155, 184842, 185527, 186209, 186889, 187567, 188242, 188915, 189586, 190255, 190921, 191585, 192247, 192907, 193565, 194221, 194875, 195527, 196177, 196824, 197469, 198113, 198755, 199395, 200033, 200669, 201303, 201935, 202565, 203193, 203819, 204444, 205067, 205688, 206307, 206925, 207541, 208155, 208767, 209378, 209987, 210594, 211200, 211804, 212406, 213007, 213606, 214204, 214800, 215394, 215987, 216578, 217168, 217756, 218343, 218928, 219512, 220094, 220675, 221255, 221833, 222410, 222985, 223559, 224131, 224702, 225272, 225840, 226407, 226973, 227537, 228100, 228662, 229222, 229781, 230339, 230895, 231450, 232004, 232557, 233108, 233658, 234207, 234755, 235302, 235847, 236391, 236934, 237476, 238017, 238556, 239094, 239631, 240167, 240702, 241236, 241769, 242300, 242830, 243359, 243887, 244414, 244940, 245465, 245989, 246512, 247034, 247555, 248075, 248594, 249112, 249629, 250145, 250659, 251172, 251684, 252706, 253216, 253725, 254233, 254740, 255246, 255751, 256255, 256758, 257260, 257761, 258261, 258760, 259258, 259755, 260251, 261241, 261735, 262228, 262720, 263211, 263701, 264190, 265166, 265653, 266139, 266624, 267108, 267591, 268555, 269036, 269516, 269995, 270951, 271428, 271904, 272379, 273327, 273800, 274272, 274743, 275683, 276152, 276620, 277554, 278020, 278485, 279413, 279876, 280338, 281260, 281720, 282638, 283096, 283553, 284465, 284920, 285828, 286281, 287185, 287636, 288536, 288985, 289881, 290328, 291220, 291665, 292553, 292996, 293880, 294321, 295201, 296079, 296517, 297391, 297827, 298697, 299565, 299998, 300862, 301724, 302154, 303012, 303868, 304722, 305148, 305998, 306846, 307692, 308114, 308956, 309796, 310634, 311470, 312304, 312720, 313550, 314378, 315204, 316028, 316850, 317670, 318488, 319304, 320118, 320930, 321740, 322548, 323354, 324158, 324960, 325760, 326957, 327753, 328547, 329339, 330129, 330917, 332096, 332880, 333662, 334442, 335609, 336385, 337159, 338317, 339087, 339855, 341004, 341768, 342911, 343671, 344808, 345564, 346695, 347447, 348572, 349320, 350439, 351555, 352297, 353407, 354514, 355250, 356351, 357449, 358544, 359272, 360361, 361447, 362530, 363610, 364687, 365761, 366832, 367900, 368965, 370027, 371086, 372142, 373195, 374245, 375292, 376336, 377724, 378762, 379797, 381173, 382202, 383228, 384592, 385612, 386968, 387982, 389330, 390338, 391678, 392680, 394012, 395340, 396333, 397653, 398969, 400281, 401589, 402893, 404193, 405165, 406457, 408067, 409351, 410631, 411907, 413179, 414447, 416027, 417287, 418543, 420108, 421356, 422911, 424151, 425696, 426928, 428463, 429993, 431213, 432733, 434248, 435758, 437263, 438763, 440258, 441748, 443233, 445009, 446484, 447954, 449419, 451171, 452626, 454366, 455811, 457539, 459261, 460977, 462402, 464106, 465804, 467496, 469182, 471142, 472816, 474484, 476146, 478078, 479728, 481646, 483557, 485189, 487086, 488976, 490859, 492735, 494604, 496466, 498321, 500433, 502274, 504370, 506197, 508277, 510349, 512413, 514212, 516516, 518556, 520588, 522612, 524880, 526888, 529138, 531379, 533363, 535586, 537800, 540250, 542446, 544633, 547053, 549463, 551623, 554013, 556393, 558763, 561359, 563709, 566283, 568613, 571165, 573706, 576236, 578755, 581491, 583988, 586700, 589400, 592088, 594764, 597428, 600301, 602941, 605788, 608622, 611660, 614468, 617478, 620260, 623242, 626210, 629375, 632315, 635450, 638570, 641675, 644971, 648251, 651311, 654762, 657994, 661411, 664611, 668193, 671559, 675105, 678633, 682143, 685635, 689302, 692950, 696770, 700380, 704160, 708108, 711848, 715754, 719639, 723687, 727713, 731899, 735881, 740021, 744317, 748589, 752837, 757237, 761612, 766136, 770634, 775106, 779723, 784483, 789215, 793919, 798762, 803742, 808692, 813776, 818829, 823851, 829164, 834444, 839691, 845221, 850559, 856175, 861755, 867453, 873267, 879043, 885083, 891083, 897043, 903259, 909580, 915858, 922238, 928718, 935439, 942113, 948881, 955741, 962691, 969867, 976991, 984335, 991760, 999264, 1006845, 1014633, 1022493, 1030423, 1038550, 1046870, 1055252, 1063694, 1072319, 1081123, 1090102, 1099130, 1108326, 1117686, 1127206, 1137000, 1146828, 1156804, 1167039, 1177413, 1188035, 1198787, 1209776, 1220886, 1232222, 1243778, 1255655, 1267633, 1279918, 1292398, 1305067, 1318021, 1331353, 1344853, 1358614, 1372628, 1386984, 1401672, 1416587, 1431909, 1447440, 1463448, 1479737, 1496387, 1513475, 1530899, 1548734, 1566966, 1585666, 1604818, 1624406, 1644496, 1665070, 1686110, 1707756, 1729986, 1752778, 1776110, 1800110, 1824752, 1850083, 1876147, 1902914, 1930494, 1958853, 1988025, 2018108, 2049128, 2081043, 2114067, 2148087, 2183179, 2219474, 2256974, 2295737, 2335873, 2377426, 2420490, 2465150, 2511428, 2559499, 2609471, 2661338, 2715388, 2771591, 2830199, 2891346, 2955148, 3021838, 3091578, 3164635, 3241159, 3321519, 3405919, 3494683, 3588201, 3686880, 3791064, 3901314, 4018172, 4142186, 4274090, 4414644, 4564704, 4725335, 4897647, 5082948, 5282810, 5498985, 5733561, 5988999, 6268201, 6574675, 6912595, 7287085, 7704397, 8172322, 8700706, 9302041, 9992577, 10793767, 11734519, 12854847, 14211627, 15888633, 18014577, 20798015, 24600641, 30110286, 38817770, 54687155, 93087155] z
	) a
  ) b, UNNEST (ind, z) AS (b_ind, b_z));    
DROP TABLE IF EXISTS xbuck;
CREATE TABLE xbuck (bucket_index bigint NOT NULL, bucket_start bigint NOT NULL, bucket_end bigint NOT NULL);
INSERT INTO xbuck
SELECT   
   b_ind - 1 bucket_index,
   b_z bucket_start, 
   LEAD(b_z-1, 1, 9223372036854775807) OVER (ORDER BY b_ind) AS bucket_end
FROM
(SELECT b_z, b_ind
FROM 
(SELECT a.z, ARRAY_ENUMERATE(a.z) ind
FROM
	(SELECT [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127, 128, 129, 130, 131, 132, 133, 134, 135, 136, 137, 138, 139, 140, 141, 142, 143, 144, 145, 146, 147, 148, 149, 150, 151, 152, 153, 154, 155, 156, 157, 158, 159, 160, 161, 162, 163, 164, 165, 166, 167, 168, 169, 170, 171, 172, 173, 174, 175, 176, 177, 178, 179, 180, 181, 182, 183, 184, 185, 186, 187, 188, 189, 190, 191, 192, 193, 194, 195, 196, 197, 198, 199, 200, 201, 202, 203, 204, 205, 206, 207, 208, 209, 210, 211, 212, 213, 214, 215, 216, 217, 218, 219, 220, 221, 222, 223, 224, 225, 226, 227, 228, 229, 230, 231, 232, 233, 234, 235, 236, 237, 238, 239, 240, 241, 242, 243, 244, 245, 246, 247, 248, 249, 250, 251, 252, 253, 254, 255, 256, 257, 259, 260, 261, 262, 263, 264, 265, 266, 267, 268, 269, 270, 271, 272, 273, 274, 276, 277, 278, 279, 280, 281, 282, 284, 285, 286, 287, 288, 289, 291, 292, 293, 294, 296, 297, 298, 299, 301, 302, 303, 304, 306, 307, 308, 310, 311, 312, 314, 315, 316, 318, 319, 321, 322, 323, 325, 326, 328, 329, 331, 332, 334, 335, 337, 338, 340, 341, 343, 344, 346, 347, 349, 351, 352, 354, 355, 357, 359, 360, 362, 364, 365, 367, 369, 371, 372, 374, 376, 378, 379, 381, 383, 385, 387, 389, 390, 392, 394, 396, 398, 400, 402, 404, 406, 408, 410, 412, 414, 416, 418, 420, 422, 425, 427, 429, 431, 433, 435, 438, 440, 442, 444, 447, 449, 451, 454, 456, 458, 461, 463, 466, 468, 471, 473, 476, 478, 481, 483, 486, 489, 491, 494, 497, 499, 502, 505, 508, 510, 513, 516, 519, 522, 525, 528, 531, 534, 537, 540, 543, 546, 549, 552, 555, 558, 562, 565, 568, 572, 575, 578, 582, 585, 589, 592, 596, 599, 603, 606, 610, 614, 617, 621, 625, 629, 633, 637, 641, 644, 648, 653, 657, 661, 665, 669, 673, 678, 682, 686, 691, 695, 700, 704, 709, 713, 718, 723, 727, 732, 737, 742, 747, 752, 757, 762, 767, 773, 778, 783, 788, 794, 799, 805, 810, 816, 822, 828, 833, 839, 845, 851, 857, 864, 870, 876, 882, 889, 895, 902, 909, 915, 922, 929, 936, 943, 950, 957, 964, 972, 979, 987, 994, 1002, 1010, 1018, 1025, 1034, 1042, 1050, 1058, 1067, 1075, 1084, 1093, 1101, 1110, 1119, 1129, 1138, 1147, 1157, 1167, 1176, 1186, 1196, 1206, 1217, 1227, 1238, 1248, 1259, 1270, 1281, 1292, 1304, 1315, 1327, 1339, 1351, 1363, 1375, 1388, 1400, 1413, 1426, 1440, 1453, 1467, 1480, 1494, 1508, 1523, 1537, 1552, 1567, 1582, 1598, 1614, 1629, 1646, 1662, 1679, 1695, 1713, 1730, 1748, 1766, 1784, 1802, 1821, 1840, 1860, 1879, 1899, 1920, 1940, 1961, 1982, 2004, 2026, 2049, 2071, 2094, 2118, 2142, 2166, 2191, 2216, 2242, 2268, 2294, 2321, 2349, 2377, 2405, 2434, 2464, 2494, 2525, 2556, 2587, 2620, 2653, 2686, 2721, 2755, 2791, 2827, 2864, 2902, 2940, 2980, 3020, 3060, 3102, 3145, 3188, 3232, 3277, 3324, 3371, 3419, 3468, 3518, 3570, 3622, 3676, 3731, 3787, 3844, 3903, 3963, 4024, 4087, 4152, 4218, 4285, 4354, 4425, 4498, 4572, 4648, 4726, 4806, 4889, 4973, 5059, 5148, 5239, 5333, 5429, 5528, 5629, 5733, 5840, 5951, 6064, 6181, 6301, 6424, 6551, 6683, 6818, 6957, 7100, 7248, 7401, 7558, 7721, 7888, 8062, 8241, 8426, 8618, 8816, 9021, 9233, 9453, 9681, 9917, 10162, 10416, 10679, 10953, 11238, 11534, 11841, 12161, 12494, 12841, 13203, 13580, 13974, 14385, 14814, 15263, 15733, 16224, 16740, 17280, 17846, 18441, 19066, 19723, 20415, 21144, 21913, 22725, 23582, 24489, 25450, 26467, 27548, 28695, 29916, 31217, 32604, 34086, 35671, 37370, 39192, 41152, 43262, 45538, 47999, 50666, 53560, 56710, 60147, 63905, 68027, 72561, 77563, 83102, 89256, 96119, 103806, 112453, 122227, 133333, 146024, 160618, 177514, 197224, 220408, 247933, 280957, 321046, 370370, 432000, 510396, 612244, 747922, 934256, 1199999, 1597633, 2231404, 3333333, 5510204, 10799999, 29999999] z
	) a
  ) b, UNNEST (ind, z) AS (b_ind, b_z));

DROP TABLE IF EXISTS ybuck; 
CREATE TABLE ybuck (bucket_index bigint NOT NULL, bucket_start bigint NOT NULL, bucket_end bigint NOT NULL);
INSERT INTO ybuck
SELECT   
   b_ind - 1 bucket_index,
   b_z bucket_start, 
   LEAD(b_z-1, 1, 9223372036854775807) OVER (ORDER BY b_ind) AS bucket_end
FROM
(SELECT b_z, b_ind
FROM 
(SELECT a.z, ARRAY_ENUMERATE(a.z) ind
FROM
	(SELECT [8216, 5809, 4743, 4108, 3674, 3354, 3105, 2905, 2739, 2598, 2477, 2372, 2279, 2196, 2121, 2054, 1993, 1936, 1885, 1837, 1793, 1752, 1713, 1677, 1643, 1611, 1581, 1553, 1526, 1500, 1476, 1452, 1430, 1409, 1389, 1369, 1351, 1333, 1316, 1299, 1283, 1268, 1253, 1239, 1225, 1211, 1198, 1186, 1174, 1162, 1150, 1139, 1129, 1118, 1108, 1098, 1088, 1079, 1070, 1061, 1052, 1043, 1035, 1027, 1019, 1011, 1004, 996, 989, 982, 975, 968, 962, 955, 949, 942, 936, 930, 924, 919, 913, 907, 902, 896, 891, 886, 881, 876, 871, 866, 861, 857, 852, 847, 843, 839, 834, 830, 826, 822, 818, 813, 810, 806, 802, 798, 794, 791, 787, 783, 780, 776, 773, 769, 766, 763, 760, 756, 753, 750, 747, 744, 741, 738, 735, 732, 729, 726, 723, 721, 718, 715, 712, 710, 707, 705, 702, 699, 697, 694, 692, 689, 687, 685, 682, 680, 678, 675, 673, 671, 669, 666, 664, 662, 660, 658, 656, 654, 652, 650, 647, 645, 644, 642, 640, 638, 636, 634, 632, 630, 628, 626, 625, 623, 621, 619, 618, 616, 614, 612, 611, 609, 607, 606, 604, 602, 601, 599, 598, 596, 594, 593, 591, 590, 588, 587, 585, 584, 582, 581, 580, 578, 577, 575, 574, 572, 571, 570, 568, 567, 566, 564, 563, 562, 560, 559, 558, 556, 555, 554, 553, 551, 550, 549, 548, 547, 545, 544, 543, 542, 541, 539, 538, 537, 536, 535, 534, 533, 531, 530, 529, 528, 527, 526, 525, 524, 523, 522, 521, 520, 519, 518, 517, 516, 514, 513, 512, 511, 510, 509, 508, 507, 506, 505, 504, 503, 502, 501, 500, 499, 498, 497, 496, 495, 494, 493, 492, 491, 490, 489, 488, 487, 486, 485, 484, 483, 482, 481, 480, 479, 478, 477, 476, 475, 474, 473, 472, 471, 470, 469, 468, 467, 466, 465, 464, 463, 462, 461, 460, 459, 458, 457, 456, 455, 454, 453, 452, 451, 450, 449, 448, 447, 446, 445, 444, 443, 442, 441, 440, 439, 438, 437, 436, 435, 434, 433, 432, 431, 430, 429, 428, 427, 426, 425, 424, 423, 422, 421, 420, 419, 418, 417, 416, 415, 414, 413, 412, 411, 410, 409, 408, 407, 406, 405, 404, 403, 402, 401, 400, 399, 398, 397, 396, 395, 394, 393, 392, 391, 390, 389, 388, 387, 386, 385, 384, 383, 382, 381, 380, 379, 378, 377, 376, 375, 374, 373, 372, 371, 370, 369, 368, 367, 366, 365, 364, 363, 362, 361, 360, 359, 358, 357, 356, 355, 354, 353, 352, 351, 350, 349, 348, 347, 346, 345, 344, 343, 342, 341, 340, 339, 338, 337, 336, 335, 334, 333, 332, 331, 330, 329, 328, 327, 326, 325, 324, 323, 322, 321, 320, 319, 318, 317, 316, 315, 314, 313, 312, 311, 310, 309, 308, 307, 306, 305, 304, 303, 302, 301, 300, 299, 298, 297, 296, 295, 294, 293, 292, 291, 290, 289, 288, 287, 286, 285, 284, 283, 282, 281, 280, 279, 278, 277, 276, 275, 274, 273, 272, 271, 270, 269, 268, 267, 266, 265, 264, 263, 262, 261, 260, 259, 258, 257, 256, 255, 254, 253, 252, 251, 250, 249, 248, 247, 246, 245, 244, 243, 242, 241, 240, 239, 238, 237, 236, 235, 234, 233, 232, 231, 230, 229, 228, 227, 226, 225, 224, 223, 222, 221, 220, 219, 218, 217, 216, 215, 214, 213, 212, 211, 210, 209, 208, 207, 206, 205, 204, 203, 202, 201, 200, 199, 198, 197, 196, 195, 194, 193, 192, 191, 190, 189, 188, 187, 186, 185, 184, 183, 182, 181, 180, 179, 178, 177, 176, 175, 174, 173, 172, 171, 170, 169, 168, 167, 166, 165, 164, 163, 162, 161, 160, 159, 158, 157, 156, 155, 154, 153, 152, 151, 150, 149, 148, 147, 146, 145, 144, 143, 142, 141, 140, 139, 138, 137, 136, 135, 134, 133, 132, 131, 130, 129, 128, 127, 126, 125, 124, 123, 122, 121, 120, 119, 118, 117, 116, 115, 114, 113, 112, 111, 110, 109, 108, 107, 106, 105, 104, 103, 102, 101, 100, 99, 98, 97, 96, 95, 94, 93, 92, 91, 90, 89, 88, 87, 86, 85, 84, 83, 82, 81, 80, 79, 78, 77, 76, 75, 74, 73, 72, 71, 70, 69, 68, 67, 66, 65, 64, 63, 62, 61, 60, 59, 58, 57, 56, 55, 54, 53, 52, 51, 50, 49, 48, 47, 46, 45, 44, 43, 42, 41, 40, 39, 38, 37, 36, 35, 34, 33, 32, 31, 30, 29, 28, 27, 26, 25, 24, 23, 22, 21, 20, 19, 18, 17, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1] z
	) a
  ) b, UNNEST (ind, z) AS (b_ind, b_z));  
DROP TABLE IF EXISTS xyzbuck;
CREATE TABLE xyzbuck (xbuck_bucket_start bigint, ybuck_bucket_start bigint, zbuck_bucket_start bigint, zbuck_bucket_end bigint)
PRIMARY INDEX zbuck_bucket_start, zbuck_bucket_end;  
INSERT INTO xyzbuck 
SELECT xbuck.bucket_start xbuck_bucket_start, ybuck.bucket_start ybuck_bucket_start, zbuck.bucket_start zbuck_bucket_start, zbuck.bucket_end zbuck_bucket_end
FROM zbuck
          JOIN xbuck ON zbuck.bucket_index = xbuck.bucket_index
          JOIN ybuck ON zbuck.bucket_index = ybuck.bucket_index;

DROP TABLE IF EXISTS page_refs;
CREATE TABLE page_refs AS 
WITH random_uniform AS  (
    SELECT 
  		random() AS U1,
  		random() AS U2
    FROM generate_series(1,  450000 ) s1(i)
),
gaussian_random AS  (
    SELECT
        sqrt(-2 * ln(U1)) * cos(2 * pi() * U2) AS gauss
    FROM random_uniform
    UNION ALL 
    SELECT
        sqrt(-2 * ln(U1)) * sin(2 * pi() * U2) AS gauss
    FROM random_uniform    
),
nextContentLength AS  (
    SELECT 
        ROUND(800 + 80 * gauss) AS content_length  -- meanContentLen + varContentLen * gauss
    FROM 
        gaussian_random
    WHERE 
        gauss >= -(800 / 80) 				-- -(meanContentLen / varContentLen)
        AND gauss <= ((32767 - 800) / 80) 	-- ((32767 - meanContentLen) / varContentLen) 
    ),
gs AS (SELECT s.idx FROM generate_series(1, 2000) AS s(idx) ),
zipcore_next AS (SELECT 
    FLOOR(RANDOM() * 93087155) AS v
FROM 
    nextContentLength n JOIN gs ON gs.idx <= n.content_length
),
pageranks AS  (
SELECT (xbuck_bucket_start + (zipcore_next.v - zbuck_bucket_start) / ybuck_bucket_start)::bigint AS id
FROM (SELECT *
  FROM xyzbuck 
  ORDER BY zbuck_bucket_start DESC 
  LIMIT 3) AS zbuck JOIN zipcore_next ON zipcore_next.v BETWEEN zbuck_bucket_start AND zbuck_bucket_end
  WHERE v >= 38817770  
UNION ALL
SELECT (xbuck_bucket_start + (zipcore_next.v - zbuck_bucket_start) / ybuck_bucket_start)::bigint AS id
FROM (SELECT *
  FROM xyzbuck 
  WHERE ybuck_bucket_start IN (4, 5, 6, 7, 8, 9, 10)) AS zbuck JOIN zipcore_next ON zipcore_next.v BETWEEN zbuck_bucket_start AND zbuck_bucket_end
  WHERE v >= 12854847 AND v < 38817770
UNION ALL  
SELECT (xbuck_bucket_start + (zipcore_next.v - zbuck_bucket_start) / ybuck_bucket_start)::bigint AS id
FROM (SELECT *
  FROM xyzbuck 
  WHERE ybuck_bucket_start IN (11,12,13,14,15,16,17,18,19,20)) AS zbuck JOIN zipcore_next ON zipcore_next.v BETWEEN zbuck_bucket_start AND zbuck_bucket_end
  WHERE v >= 6574675 AND v < 12854847 
UNION ALL    
  SELECT (xbuck_bucket_start + (zipcore_next.v - zbuck_bucket_start) / ybuck_bucket_start)::bigint AS id
FROM xyzbuck AS zbuck JOIN zipcore_next ON zipcore_next.v BETWEEN zbuck_bucket_start AND zbuck_bucket_end
  WHERE v < 6574675
  )
SELECT id, count(*) refs
FROM pageranks 
GROUP BY ALL;
-- 1m 9s on 1XL engine

select sum(refs) from page_refs where id in (select id from urls_only);

DROP TABLE IF EXISTS url_id_refs;
CREATE TABLE url_id_refs (id bigint NOT NULL) PRIMARY INDEX id;
INSERT INTO url_id_refs
SELECT id FROM page_refs join generate_series(1, 10) s(i) ON page_refs.refs >= i
UNION ALL
SELECT id FROM page_refs join generate_series(11, 100) s(i) ON page_refs.refs >= i
WHERE page_refs.refs BETWEEN 11 AND 4000
UNION ALL
SELECT id FROM page_refs join generate_series(101, 500) s(i) ON page_refs.refs >= i
WHERE page_refs.refs BETWEEN 101 AND 4000
UNION ALL
SELECT id
FROM page_refs join generate_series(501, 1000) s(i) ON page_refs.refs >= i
WHERE page_refs.refs BETWEEN 501 AND 4000 
UNION ALL
SELECT id
FROM page_refs join generate_series(1001, 4000) s(i) ON page_refs.refs >= i
WHERE page_refs.refs BETWEEN 1001 AND 4000
UNION ALL
SELECT id 
FROM (SELECT id, generate_series(1,refs) FROM page_refs WHERE page_refs.refs > 4000)		
;
-- 34s on 1XL engine

DROP TABLE IF EXISTS urls_only;
CREATE TABLE urls_only AS
SELECT  id,
  		(SELECT ARRAY_TO_STRING(ARRAY_AGG(SUBSTRING('abcdefghijklmnopqrstuvwxyz' FROM FLOOR(RANDOM() * 26)::int FOR 1 )))
        FROM generate_series(1, FLOOR(RANDOM() * (90 - 10 + 1) + 10 + id*0)::int)) as url -- nextUrlLength() 
from generate_series(1, 30000000 /*pages*/) s(id); 
-- 53s on 1XL engine

DROP TABLE IF EXISTS rawuagents;
CREATE TABLE rawuagents (weight double, agent_string TEXT );
INSERT INTO rawuagents VALUES 
			(0.0800, 	'Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; .NET CLR 1.1.4322)'),
			(0.0300, 	'Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.0)'),
			(0.1300, 	'Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1)'),
			(0.0900, 	'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.2)'),
			(0.0700, 	'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.0)'),
			(0.0700,	'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'),
			(0.0400,	'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1)'),
			(0.0150, 	'Mozilla/4.0 (compatible; MSIE 5.5; Windows NT 5.0)'),
			(0.0080, 	'Mozilla/4.0 (compatible; MSIE 5.5; Windows 98; Win 9x 4.90)'),
			(0.0600, 	'Mozilla/5.0 (Windows; U; Windows NT 5.2) AppleWebKit/525.13 (KHTML like Gecko) Chrome/xxx'),
			(0.0300, 	'Mozilla/5.0 (Windows; U; Windows NT 5.2)AppleWebKit/525.13 (KHTML like Gecko) Version/3.1Safari/525.13'),
			(0.0400, 	'Mozilla/5.0 (iPhone; U; CPU like Mac OS X)AppleWebKit/420.1 (KHTML like Gecko) Version/3.0 Mobile/4A93Safari/419.3'),
			(0.0050, 	'iPhone 3.0: Mozilla/5.0 (iPhone; U; CPU iPhone OS 3_0 like Mac OS X; en-us) AppleWebKit/528.18 (KHTML like Gecko) Version/4.0 Mobile/7A341 Safari/528.16'),			
  			(0.0050, 	'Mozilla/5.0 (Macintosh; U; PPC Mac OS X; en) AppleWebKit/125.2 (KHTML like Gecko) Safari/125.8'),
			(0.0030,	'Mozilla/5.0 (Macintosh; U; PPC Mac OS X; en) AppleWebKit/125.2 (KHTML like Gecko) Safari/85.8'),
			(0.0080, 	'Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.8.1.12)Gecko/20080219 Firefox/2.0.0.12	Navigator/9.0.0.6'),
			(0.0070, 	'Mozilla/5.0 (Windows; U; Windows NT 5.2)Gecko/2008070208 Firefox/3.0.1'),
			(0.0080, 	'Mozilla/5.0 (Windows; U; Windows NT 5.1)Gecko/20070309 Firefox/2.0.0.3'),
			(0.0060, 	'Mozilla/5.0 (Windows; U; Windows NT 5.1)Gecko/20070803 Firefox/1.5.0.12'),
			(0.0112, 	'Mozilla/5.0 (Windows; U; Windows NT 5.2) AppleWebKit/525.13 (KHTML like Gecko) Chrome/0.2.149.27 Safari/525.13'),
			(0.0075, 	'Netscape 4.8 (Windows Vista): Mozilla/4.8 (Windows NT 6.0; U)'),
			(0.0033,	'Opera 9.2 (Windows Vista): Opera/9.20 (Windows NT 6.0; U; en)'),
			(0.0028, 	'Opera 8.0 (Win 2000): Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.0; en) Opera 8.0'),
			(0.0035, 	'Opera 7.51 (Win XP): Opera/7.51 (Windows NT 5.1; U)'),
			(0.0030, 	'Opera 7.5 (Win XP): Opera/7.50 (Windows XP; U)'),
			(0.0020, 	'Opera 7.5 (Win ME): Opera/7.50 (Windows ME; U)'),
			(0.0012,	'Opera/9.27 (Windows NT 5.2; U; zh-cn)'),
			(0.0048,	'Opera/8.0 (Macintosh; PPC Mac OS X; U; en)'),
			(0.0026, 	'Mozilla/5.0 (Macintosh; PPC Mac OS X; U; en)Opera 8.0'),
			(0.0025, 	'Netscape 7.1 (Win 98): Mozilla/5.0 (Windows; U; Win98; en-US; rv:1.4) Gecko Netscape/7.1 (ax)'),
			(0.0035, 	'Netscape 4.8 ( Win XP): Mozilla/4.8 (Windows NT 5.1; U)'),
			(0.0010, 	'Netscape 3.01 gold (Win 95): Mozilla/3.01Gold (Win95; I)'),
			(0.0020, 	'Netscape 2.02 (Win 95): Mozilla/2.02E (Win95; U)'),
			(0.2441,	'***')
	;
DROP TABLE IF EXISTS agents;
CREATE TABLE agents (id int, agentname text, operatingsystem text, devicearch text, browser text);
INSERT INTO agents (id, agentname, operatingsystem, devicearch, browser)  
SELECT 
	id,
  	agentname,
  	CASE WHEN POSITION('Windows' IN agentname) > 0 THEN 'Windows 10' ELSE 'macOS' END AS operatingsystem,  
    CASE 
    	WHEN random() < 0.33 THEN 'x64'
        WHEN random() < 0.66 THEN 'x86'
        ELSE 'ARM'
    END AS devicearch,
    CASE
    	WHEN POSITION('MSIE' IN agentname) > 0 THEN 'Internet Explorer'
        WHEN POSITION('Firefox' IN agentname) > 0 THEN 'Mozilla Firefox'
        WHEN POSITION('Chrome' IN agentname) > 0 THEN 'Google Chrome'
        WHEN POSITION('Safari' IN agentname) > 0 AND POSITION('Chrome' IN agentname) = 0 THEN 'Safari'
        WHEN POSITION('Opera' IN agentname) > 0 THEN 'Opera'
        ELSE 'Other'
    END AS browser
FROM (SELECT 
  	row_number() OVER () AS id,
  	CASE WHEN agent_string = '***' THEN  -- content = content + nextSeedAgent() + "\n";
  			(SELECT 
  					SUBSTRING('ABCDEFGHIJKLMNOPQRSTUVWXYZ' FROM FLOOR(RANDOM() * 26)::int FOR 1 )||
  					ARRAY_TO_STRING(ARRAY_AGG(SUBSTRING('abcdefghijklmnopqrstuvwxyz' FROM FLOOR(RANDOM() * 26)::int FOR 1 )))||
  					'/'||SUBSTRING('0123456789' FROM FLOOR(RANDOM() * 10)::int FOR 1)||'.'||SUBSTRING('0123456789' FROM FLOOR(RANDOM() * 10)::int FOR 1)
        	FROM generate_series(1, FLOOR(RANDOM()*20 + 5 + id*0)::int))   -- int len = rand.nextInt(20) + 5;
  		 ELSE agent_string 
    END AS agentname
FROM (SELECT row_number() OVER () AS id, agent_string
      FROM (SELECT agent_string,
  	   	           generate_series(1, (weight*2000)::int) -- numSourceUAgents = 2000; int num = (int) Math.round(Double.parseDouble(pair[0]) * numSourceUAgents);  
            FROM rawuagents)));

DROP TABLE IF EXISTS ccodes;
CREATE TABLE ccodes (id int, countrycode text, languagecode text);
INSERT INTO ccodes VALUES
(1, 'ARE','ARE-AR'),(2, 'JOR','JOR-AR'),(3, 'SYR','SYR-AR'),(4, 'HRV','HRV-HR'),(5, 'BEL','BEL-FR'),(6, 'PAN','PAN-ES'),(7, 'MLT','MLT-MT'),(8, 'VEN','VEN-ES'),
(9, 'TWN','TWN-ZH'),(10, 'DNK','DNK-DA'),(11, 'PRI','PRI-ES'),(12, 'VNM','VNM-VI'),(13, 'USA','USA-EN'),(14, 'MNE','MNE-SR'),(15, 'SWE','SWE-SV'),(16, 'BOL','BOL-ES'),
(17, 'SGP','SGP-EN'),(18, 'BHR','BHR-AR'),(19, 'SAU','SAU-AR'),(20, 'YEM','YEM-AR'),(21, 'IND','IND-HI'),(22, 'MLT','MLT-EN'),(23, 'FIN','FIN-FI'),(24, 'BIH','BIH-SR'),
(25, 'UKR','UKR-UK'),(26, 'CHE','CHE-FR'),(27, 'ARG','ARG-ES'),(28, 'EGY','EGY-AR'),(29, 'JPN','JPN-JA'),(30, 'SLV','SLV-ES'),(31, 'BRA','BRA-PT'),(32, 'ISL','ISL-IS'),
(33, 'CZE','CZE-CS'),(34, 'POL','POL-PL'),(35, 'ESP','ESP-CA'),(36, 'MYS','MYS-MS'),(37, 'ESP','ESP-ES'),(38, 'COL','COL-ES'),(39, 'BGR','BGR-BG'),(40, 'BIH','BIH-SR'),
(41, 'PRY','PRY-ES'),(42, 'ECU','ECU-ES'),(43, 'USA','USA-ES'),(44, 'SDN','SDN-AR'),(45, 'ROU','ROU-RO'),(46, 'PHL','PHL-EN'),(47, 'TUN','TUN-AR'),(48, 'MNE','MNE-SR'),
(49, 'GTM','GTM-ES'),(50, 'KOR','KOR-KO'),(51, 'CYP','CYP-EL'),(52, 'MEX','MEX-ES'),(53, 'RUS','RUS-RU'),(54, 'HND','HND-ES'),(55, 'HKG','HKG-ZH'),(56, 'NOR','NOR-NO'),
(57, 'HUN','HUN-HU'),(58, 'THA','THA-TH'),(59, 'IRQ','IRQ-AR'),(60, 'CHL','CHL-ES'),(61, 'MAR','MAR-AR'),(62, 'IRL','IRL-GA'),(63, 'TUR','TUR-TR'),(64, 'EST','EST-ET'),
(65, 'QAT','QAT-AR'),(66, 'PRT','PRT-PT'),(67, 'LUX','LUX-FR'),(68, 'OMN','OMN-AR'),(69, 'ALB','ALB-SQ'),(70, 'DOM','DOM-ES'),(71, 'CUB','CUB-ES'),(72, 'NZL','NZL-EN'),
(73, 'SRB','SRB-SR'),(74, 'CHE','CHE-DE'),(75, 'URY','URY-ES'),(76, 'GRC','GRC-EL'),(77, 'ISR','ISR-IW'),(78, 'ZAF','ZAF-EN'),(79, 'THA','THA-TH'),(80, 'FRA','FRA-FR'),
(81, 'AUT','AUT-DE'),(82, 'NOR','NOR-NO'),(83, 'AUS','AUS-EN'),(84, 'NLD','NLD-NL'),(85, 'CAN','CAN-FR'),(86, 'LVA','LVA-LV'),(87, 'LUX','LUX-DE'),(88, 'CRI','CRI-ES'),
(89, 'KWT','KWT-AR'),(90, 'LBY','LBY-AR'),(91, 'CHE','CHE-IT'),(92, 'DEU','DEU-DE'),(93, 'DZA','DZA-AR'),(94, 'SVK','SVK-SK'),(95, 'LTU','LTU-LT'),(96, 'ITA','ITA-IT'),
(97, 'IRL','IRL-EN'),(98, 'SGP','SGP-ZH'),(99, 'CAN','CAN-EN'),(100, 'BEL','BEL-NL'),(101, 'CHN','CHN-ZH'),(102, 'JPN','JPN-JA'),(103, 'GRC','GRC-DE'),(104, 'SRB','SRB-SR'),
(105, 'IND','IND-EN'),(106, 'LBN','LBN-AR'),(107, 'NIC','NIC-ES'),(108, 'MKD','MKD-MK'),(109, 'BLR','BLR-BE'),(110, 'SVN','SVN-SL'),(111, 'PER','PER-ES'),(112, 'IDN','IDN-IN'),
(113, 'GBR','GBR-EN');

DROP TABLE IF EXISTS searchkeys;
CREATE TABLE searchkeys (id int, word text, word_hash bigint, word_id bigint, firstseen pgdate, is_topic boolean);
INSERT INTO searchkeys (id, word)  
SELECT 
  		id,
		(SELECT ARRAY_TO_STRING(ARRAY_AGG(SUBSTRING('abcdefghijklmnopqrstuvwxyz' FROM FLOOR(RANDOM() * 26)::int FOR 1 )))
        FROM generate_series(1, FLOOR(RANDOM()*15 + 3 + id*0)::int)) AS word  -- nextSeedWord()         
FROM generate_series(1, 1000::int) AS g(id) -- numSourceWords = 1000;  
;

DROP TABLE IF EXISTS uservisits;
CREATE TABLE uservisits (sourceip text NOT NULL, destinationurl text NOT NULL, visitdate pgdate NOT NULL, 
  adrevenue real NOT NULL, useragent text NOT NULL, countrycode text NOT NULL, languagecode text NOT NULL, 
  searchword text NOT NULL, duration integer NOT NULL) 
PRIMARY INDEX visitdate, destinationurl, sourceip;
INSERT INTO uservisits (sourceip, destinationurl, visitdate, adrevenue, useragent, countrycode, 
  			languagecode, searchword, duration)
SELECT 
        FLOOR(RANDOM() * 254 + 1)::INT || '.' ||
    	FLOOR(RANDOM() * 255)::INT || '.' ||
    	FLOOR(RANDOM() * 255)::INT || '.' ||
    	FLOOR(RANDOM() * 254 + 1)::INT AS sourceip, -- nextIp
		u.url AS destinationurl,
		DATE_ADD('second', 
        	(RANDOM() * DATE_DIFF('second','1970-01-01'::DATE, '2012-05-01'::DATE))::INT,
  			'1970-01-01'::DATE) AS visitdate, -- nextDate
		ROUND(RANDOM(),9) AS adrevenue, -- nextProfit
  		agents.agentname AS useragent,	  -- nextUserAgent
		ccodes.countrycode, -- nextCountryCode,
  		ccodes.languagecode,
		searchkeys.word AS searchword, -- nextSearchKey,
		(FLOOR(RANDOM() * 10) + 1)::INT AS duration -- nextTimeDuration
FROM urls_only u LEFT JOIN url_id_refs ON u.id = url_id_refs.id 
  	 JOIN agents ON mod(u.id, 2000) + 1 = agents.id
     JOIN ccodes ON mod(u.id, 113) + 1 = ccodes.id
     JOIN searchkeys ON mod(u.id, 1000) + 1 = searchkeys.id
UNION ALL 
SELECT 
        FLOOR(RANDOM() * 254 + 1)::INT || '.' ||
    	FLOOR(RANDOM() * 255)::INT || '.' ||
    	FLOOR(RANDOM() * 255)::INT || '.' ||
    	FLOOR(RANDOM() * 254 + 1)::INT AS sourceip, -- nextIp,
		u.url,
		DATE_ADD('second', 
        	(RANDOM() * DATE_DIFF('second','1970-01-01'::DATE, '2012-05-01'::DATE))::INT,
  			'1970-01-01'::DATE) AS nextDate,
		ROUND(RANDOM(),9) AS adrevenue, -- nextProfit
		agents.agentname AS useragent,	  -- nextUserAgent
		ccodes.countrycode, -- nextCountryCode,
  		ccodes.languagecode,
		searchkeys.word AS searchword, -- nextSearchKey,
		(FLOOR(RANDOM() * 10) + 1)::INT AS duration -- nextTimeDuration
FROM urls_only u 
     JOIN agents ON mod(u.id, 2000) + 1 = agents.id
  	 JOIN ccodes ON mod(u.id, 113) + 1 = ccodes.id
     JOIN searchkeys ON mod(u.id, 1000) + 1 = searchkeys.id  
  ;  
-- 2m 7s on 1XL engine

show tables uservisits;

VACUUM uservisits;
-- 4m 9s on 1XL engine

DROP AGGREGATING INDEX IF EXISTS idx_by_day;
CREATE AGGREGATING INDEX idx_by_day ON uservisits (
  visitdate,
  countrycode,
  languagecode,
  useragent,
  MAX(visitdate),
  SUM(adrevenue),
  MAX(adrevenue),
  COUNT(*)
);
-- 3m 33s

DROP TABLE IF EXISTS rankings;
CREATE TABLE rankings (pageurl text, pagerank integer, avgduration integer) PRIMARY INDEX pageurl;
INSERT INTO rankings (pageurl, pagerank, avgduration) 
SELECT 
  	u.url AS pageurl, 
    page_refs.refs AS pagerank,
    FLOOR(RANDOM() * 99) + 1 AS avgduration
FROM urls_only u LEFT JOIN page_refs ON u.id = page_refs.id; 
-- 23s on 1XL engine

VACUUM rankings;
-- 0.07s

DROP TABLE IF EXISTS searchwords;
CREATE TABLE searchwords (word TEXT NOT NULL, word_hash bigint NOT NULL, word_id bigint NOT NULL, firstseen DATE NOT NULL, is_topic boolean NOT NULL);
INSERT INTO searchwords (word, word_hash, word_id, firstseen, is_topic)
SELECT
    searchword AS word,
    city_hash(searchword) AS word_hash,
    floor(random() * 9223372036854775807)::bigint AS word_id,
    firstseen,
    (random() < 0.5) AS is_topic
FROM (SELECT searchword, min(visitdate) AS firstseen 
      FROM uservisits
      GROUP BY ALL);

DROP TABLE IF EXISTS ipaddresses;
CREATE TABLE ipaddresses (ip text NOT NULL, autonomoussystem integer NOT NULL, asname text NULL) PRIMARY INDEX ip;
INSERT INTO ipaddresses (ip, autonomoussystem, asname)
WITH b AS MATERIALIZED
    (WITH a AS MATERIALIZED (SELECT ROW_NUMBER() OVER (ORDER BY sourceip) AS rownum
                            FROM (SELECT sourceip FROM uservisits LIMIT 255))
      SELECT 
  			prefix, 
  			ROW_NUMBER() OVER (ORDER BY prefix) AS autonomoussystem
      FROM (SELECT a1.rownum::TEXT||'.'||a2.rownum::TEXT||'.' prefix
            FROM a AS a1 JOIN a AS a2 ON 1 = 1))
     SELECT u.sourceip, 
  			b.autonomoussystem,
  			substr(md5(random()::text || b.autonomoussystem::text), 1, (random() * 20 + 5)::int) asname
     FROM (SELECT DISTINCT sourceip FROM uservisits) u JOIN b on split_part(u.sourceip, '.', 1)||'.'||split_part(u.sourceip, '.', 2)||'.' = b.prefix;

VACUUM searchwords;
VACUUM ipaddresses;
VACUUM agents;


DROP TABLE IF EXISTS zbuck;
DROP TABLE IF EXISTS xbuck;
DROP TABLE IF EXISTS ybuck; 
DROP TABLE IF EXISTS xyzbuck;
DROP TABLE IF EXISTS page_refs;
DROP TABLE IF EXISTS url_id_refs;
DROP TABLE IF EXISTS rawuagents;
DROP TABLE IF EXISTS ccodes;
DROP TABLE IF EXISTS searchkeys;
DROP TABLE IF EXISTS urls_only;

SHOW TABLES;


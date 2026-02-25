**What is does**

Automatically merge gorilla output with calculation on psychometric scales.

Currently support:
1. CAPE-15
2. DASS-21-Depression
3. GAD-7
4. CannabisBackground (mggt in Gorilla)
5. CannabisUseMotive
6. CannabisUseSelfIdentity
7. CUDIT-R
8. I-8
9. Lifetime Cannabis Use
10. Demographics

**How to use**

1. Download all data files from Gorilla via **Long Form**
2. Rename files. Use names I gave above
3. Run the script.
4. Choose Lifetime cannabis use as the base file, then you will be asked if you want to merge more files
5. Select Yes, select all other files, and click Yes
6. Check OUTPUT **(IMPORTANT: Always check every variable and do some confirmative test to see if the results are correct)**

**What will happen if I merge a data file that does not belong to the above list?**

The script will automatically merge all columns into the base file. I do not recommend you to do so because there should be multiple rows for one participant and I suppose only the results in the last row will be merged.
# mpi4py
##Pseudocode for the serial program
	1. Initialize Step
		1. Create a global variable time steps T=10 and a numpy array ‘global_avg’ of size T initialized to 0. This will contain the average of all data frames across all time steps.
		2. Create a variable N=8.
		3. Set value of variable R=1.
		4. Create a dictionary ‘dfs_meta’ with a single key that equals the data frame id and with value equal to another dictionary with 4 key/value pairs. The latter dictionary will contain meta-data for a single data-frame. The 1st key is rows to contain the number of rows, 2nd key is cols to contain the number of columns, the 3rd key is average to contain the average of all elements of the data frame and the 4th key will be a pointer to a numpy array that will contain the data of the data frame.
		5. Connect to RDS. Read meta data and re-initialize the value of N. Load data for N/R data frames. 
		6. For each data frame
			1. Load meta-data into the dfs_meta data structure.
			2. Load the data into a numpy array of size (20, 20).
	2. Begin a time step loop for 10 time steps.
		1. Create a variable ‘iteration_avg’ initialized to 0. This will contain the average of all data frames for a single iteration.
		2. For each data frame
			1. Resize the array to (rows, cols+1)
			2. Compute the average for each row and save in the element at the end of the row.
			3. Compute the average of all data frames, including their new column, and save it in the ‘dfs_meta’ data-structure. Also, increment the number of columns in ‘dfs_meta’.
		3. Compute ‘iteration_avg’ by taking the average of all data-frames.
		4. Update ‘global_avg’ and add the value of ‘iteration_avg’.
	3. End time step loop
	4. Compute the mean of the ‘global_avg’ array and enter its value in the ‘analytics’ table in the RDS database.
	5. Write data frames to RDS.
		1. For each data frame
			1. Update the meta-data table and update the ‘cols’ column.
			2. Delete all data from the ‘data’ table associated with the data-frame id
			3. Write all data to the ‘data’ table in RDS associated with the data-frame id.

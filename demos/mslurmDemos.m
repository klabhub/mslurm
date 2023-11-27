%% Tutorial 
%% If you followed the readme, you already ran mslurm.install to set preferences 
% for connecting to a cluster. This call uses those preferences
cls = mslurm; 
cls.connect; % Connect
cls.sacct; % Update ths cls object with the current state of the accounting by SLURM (e.g.. the list of running jobs)
cls.sinfo  % Show what the cluster has to offer at this time (see manual for slurm sinfo)
cls.batchOptions = {'time','00:10:00','mem','8GB','cpus-per-task',1};  % Default options for all jobs

% Open the App  to see the jobs that the SLURM schedulre knows about (For
% the current user). This could be empty if you did not schedule any jobs
% yet (or if they were scheduled before the date selected in the calender).
mslurmApp(cls);


%% Now schedule a simple job. We'll generate 5 matrices of 10x10  random
% numbers.
nrWorkers = 5;
randData = 10*ones(nrWorkers,1);
options = {'time',10}; % Ask for 10 minutes of wall time.
jobName = cls.remote("rand",'funData',randData,'batchOptions',options); % This will call rand(data(1)) in one matlab sesssion, rand(data(2)) in another etc.
% Click refresh in the mslurmApp to see these jobs, or use cls.sacct
% Once they have completed, you can retrieve the results with 
results = cls.retrieve(jobName);

%% Another example, using  a data struct array
% We have reaction time data from 3 subjects.
rtData = struct('name',{'Joe','Bill','Mary'},'rt',{[200 300 100],[200 333 1123],[123 300 200]});
% We want to use a cluster to analyze the data from each subject in a separate job.
rtJob = cls.remote("analyzeRt",'funData',rtData,'copy',true);
% The analyzeRt m functon is a simple function that takes one of the
% elements of the struct array as its input, and computes the mean reaction
% time. Because that function won't be available on the cluster, we set
% 'copy' to true. 
%
% Once the jobs complete, retrieve the data. Each item in the cell array
% correponds to the output of a single job (a subject here).
meanRt = cls.retrieve(rtJob);


%% Copying code 
% Although the remote function allows you to copy an individual script/function to the cluster 
% there is no built-in tool to keep entire folders synchronized.For data I'd recommend
% using rsync to achieve this. But for code, github is more
% convenient. For instance, youi can install your git repo  on the client, but also
% on the server. Then after committing local changes and pushing them to
% github, you can call:
myGithub = '~/myproject';
cls.gitpull(myGithub);  % Pull from the origin on git.
% If you do this before you submit a job, the code on the cluster will
% match the code on  the client. 

%% Troubleshooting, 
% The slurmDiagnosis function can come in handy. By default it returns
% information on the Matlab installation on the cluster, but you can edit/update 
% it with your own diagnostic commands.
diagnosticTag = cls.remote('slurmDiagnose','basic');
% Once completed, load the output file by pressing 'o' in the gui , or
% manually with the getFile function:
cls.getFile(diagnosticTag)





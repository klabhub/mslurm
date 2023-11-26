
%% If you followed the readme, you already ran mslurm.install to set preferences 
% for connecting to a cluster. This call uses those preferences
cls = mslurm; 
cls.connect; % Connect
cls.sacct; % Update ths cls object with the current state of the accounting by SLURM (e.g.. the list of running jobs)
cls.sinfo  % Show what the cluster has to offer at this time (see manual for slurm sinfo)
cls.addPath = "/home/bart/devmslurm";% Add the path to the mslurm installation on the cluster.
cls.batchOptions = {'time','00:10:00','mem','8GB','cpus-per-task',1};  % Default options for all jobs

% Open the App  to see the jobs that the SLURM schedulre knows about (For
% the current user). This could be empty if you did not schedule any jobs
% yet (or if they were scheduled before the date selected in the calender).
mslurmApp(cls);


%% Now schedule a simple job. We'll generate 5 matrices of 10x10  random
% numbers.
nrWorkers = 5;
data = 10*ones(nrWorkers,1);
options = {'time',10}; % Ask for 10 minutes of wall time.
jobName = cls.remote("rand",'funData',data,'batchOptions',options); % This will call rand(data(1)) in one matlab sesssion, rand(data(2)) in another etc.
% Click refresh in the mslurmApp to see these jobs, or use cls.sacct
% Once they have completed, you can retrieve the results with 
results = cls.retrieve(jobName);

%% Another example, using  a data struct array
% We have reaction time data from 3 subjects.
data = struct('name',{'Joe','Bill','Mary'},'rt',{[200 300 100],[200 333 1123],[123 300 200]});
% We want to use a cluster to analyze the data from each subject in a separate job.
remote% The analyzeRt m functon is a simple function that takes one of the
% elements of the struct array as its input, and computes the mean reaction
% time. Because that function won't be available on the cluster, we set
% 'copy' to true. 
%
% Once the jobs complete, retrieve the data. Each item in the cell array
% correponds to the output of a single job (a subject here).
meanRt = cls.retrieve(rtJob);


%% Use fileInFileOut
% Another variant of cluster based jobs takes a list of files, processes
% them with some function, and saves the results (on the cluster) in a new
% file. 
% Define a list (string array) of files to "analyze" (these files are in the matlab demos
% directory so they are likely to exist on the cluster)
files= ["earth.mat","flujet.mat","detail.mat","durer.mat"];
% Because these files exist on the Matlab path, we can set inPath to "" 
% The results will be stored in the OutPath directory (we choose
% cls.remoteStorage)
% and the OutTag will be appended to the result files : the result of
% slurmAnalyzeFile('earth.mat') will be stored as earth.whos.mat in the
% cls.remoteStorage directory
remotePath = cls.fileInFileOut('analyzeFile','InFile',files,'OutTag',".whos");

% There is no built-in,automatic way to retrieve these data,it is assumed that you do
% this at the OS level (e.g. with rsync or scp). But we can have a look at the 
% output directory with a simple unix command.
cls.command(sprintf("ls %s/*.whos.mat",remotePath))
% And, because we know the file names, we can retrieve them manually
resultFiles = strrep(files,'.mat','.whos.mat');
cls.get(resultFiles,"c:\temp",remotePath);% 
%This will get the files from remotePath and put them in c:\temp
%(You can specify deleteRemote argument to remove the files
%from cluster storage)
% 
% Now we can open one of the files
load(fullfile("c:\temp",resultFiles(1)));
% This will put a variable called 'result' in the current workspace, which
% contains the result of analyzeFile(files(1). We can now use the
% results of this "analysis" of the file:
fprintf("The  file %s contains %d variables with a total of %g bytes ", files(1),numel(result)) ,sum([result.bytes]);

%% Copying code 
% Although some functions allow you to copy an individual script/function to the cluster 
% there is no built-in tool to keep entire folders synchronized.For data I'd recommend
% using rsync to achieve this. But for code github is more
% convenient.For instance, install your git repo  on the client, but also
% on the server. Then after committing local changes and pushing them to
% github, you can call:
myGithub = '~/myproject';
cls.gitpull(myGithub);  % Pull from the origin on git.
% If you do this before you submit a job, the code on the cluster will
% match the code on  the client. 
%% Troubleshooting, the slurmDiagnosis function can come in handy
% (you can edit/update it with your own diagnostic commands).
diagnosticTag = cls.feval('slurmDiagnose','basic');
% Once completed, load the output file by pressing 'o' in the gui.
cls.getFile(diagnosticTag)





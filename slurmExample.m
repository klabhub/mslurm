
cls = slurm;
cls.host = 'nm3hpc.newark.rutgers.edu';  % The host name of the cluster
cls.localStorage = 'c:\temp';            % Staging area on the local client machine
cls.user = '';  % Your user name on the cluster
cls.keyfile = ''; %Provide the path to your RSA SSH auth file here.
cls.remoteStorage = '/work/klab/jobStorage/';   % Staging area on the cluster, also the working directory for Matlab when it starts on the cluster
cls.nodeTempDir = '/scratch/';              % Matlab will write temporary results here on the nodes.
cls.headRootDir = '/work/klab/';            % Final resuls will be copied here from the node.    

cls.connect; % Connect
cls.sacct; % Get the current accounting state 
% Open the GUI to see the jobs that the SLURM schedulre knows about (For
% the current user). This could be empty if you did not schedule any jobs
% yet (or if they were scheduled before the date selected in the calender).

slurmGui(cls);


% Now schedule a simple job. We'll generate 5 matrices of 10x10  random
% numbers.
nrWorkers = 5;
data = 10*ones(nrWorkers,1);
options = {'partition','test'}; % Specify a partition and antyhing else the sbatch will accept (e.g. memory requirements)
tag = cls.feval('rand',data,'batchOptions',options); % This will call rand(data(1)) in one matlab sesssion, rand(data(2)) in another etc.

% Another example, using  a data struct array

% We have reaction time data from 3 subjects.
data = struct('name',{'Joe','Bill','Mary'},'rt',{[200 300 100],[200 333 1123],[123 300 200]});
% We want to use a cluster to analyze the data from each subject in a separate job.
tag = s.feval('slurmAnalyzeRt',data);
% See slurmAnalyzeRt for 


% Click refresh in the slurmGui to see these jobs
% Once they have completed, you can retrieve the results with 
results = cls.retrieve(tag);


% Here's another useful function to make sure your mslurm installation on
% the server is up to date
mslurmPath = '~/Documents/MATLAB/mslurm';
cls.gitpull(mslurmPath);  % Pull from the origin on git.

% And for troubleshooting, the slurmDiagnosis function can come in handy
% (you can edit/update it with your own diagnostic commands).
cls.feval('slurmDiagnose','basic')
% Once completed, load the output file by pressing 'o' in the gui.





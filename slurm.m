% A class to interact with a remote SLURM cluster using SSH
%
% Depends on the SSH2 Package available on the Matlab FileExchange.
% See readme.md for installation instructions and slurmExample for an
% example.
%
% EXAMPLE:
% Setup a connection to  a cluster
% s =slurm;
% s.host = 'wickedfast.university.edu';
% s.user = 'username'
% s.keyfile = 'mySSH_id_rsa'
% slurm.m creates files that are stored in .localStorage
% s.localSotrage  = 'c:/temp/jobStorage/'; 
% These files are copied to the cluster in .remoteStorage
% s.remoteStorage = '~/jobStorage/'
% s.nodeTempDir = '/scratch/' % A directory where results can be written on the nodes 
% before they are copied to the head node)
% s.connect ; % Check that we can connect.
% % Assume that the mfile 'myComputation.m' exists on the cluster, it takes
% a structure with data as its input and returns a single 'result' We have
% an array of data (i.e. data(1)..data(N) and want to evaluate
% 'myComputation' for each of these data sets. To achieve this, type:
% tag = s.feval('myComputation',data);
% This will start one worker/job for each element of data. 'tag' identifies
% this partticular set of jobs. 
% 
% Once the jobs have completed you retrieve the results using 
%  results = s.retrieve(tag)
%
% See also slurmGui.
%
% BK - Jan 2015
% June 2017 - Public release.

classdef slurm < handle
    
    properties (SetAccess=public, GetAccess=public)
        remoteStorage@char  = ''; % Location on HPC Cluster where scripts and logs will be written
        localStorage@char   = ''; % Location on client where logs and scripts will be written 
        host@char           = ''; % Host Address
        user@char           = ''; % Remote user name
        keyfile@char        = ''; % Name of the SSH key file. (full name)
        from                = now - 1; % Retrieve logs from this time (use datenum format)
        to                  = Inf; % Until this time.
        nodeTempDir         = '';  % The path to a directory on a node that can be used to save data temporarily (e.g. /scratch/)
        headRootDir         = '';  % The path to the directory on the head node where results can be copied        
    end
    
    properties (Dependent,SetAccess=protected)
        pwd;                    % Working directory on the SLURM cluster
        nrJobs;                 % Number of jobs currently available in the log (i.e. submitted between .from and .to)
        isConnected;            % Check that we have an open SSH connection to the cluster.
        failState;              % Current state of each of the jobs in the log.
    end
    properties (SetAccess=protected, GetAccess=public)
        jobs;                    % A structure with the information retrieved from the slurm accounting logs (see the sacct function below)
    end
    
    properties (SetAccess=protected, GetAccess=public, Transient)
        ssh@struct;             % An SSH2 structure
    end
    
    %% Get/set methods for dependent properties
    methods
        function v=get.pwd(o)
            v = command(o,'pwd');
        end
        function  v=get.nrJobs(o)
            v = length(o.jobs);
        end
        function v=get.isConnected(o)
            v = ~isempty(o.ssh.connection);
        end
        
        function state = get.failState(o)
            % Determine the number of failures for each job in the current
            % job log.
            % A job that ran successfully has a fail state of 0
            % A job that failed and was completed successfully later has a
            % state of -1
            % A job that is currently running has a state of -2
            % All other jobs have a state that corresponds to the
            % cumulative number of attempts.
            
            failedStates = {'CANCELLED','FAILED','NODE_FAIL','PREEMPTED','SUSPENDED','TIMEOUT'};
            %notFailedStates = {'COMPLETED','CONFIGURING','COMPLETING','PENDING','RUNNING','RESIZING'};
            
            % Find the failed jobs
            allFailures = ~strcmpi('0:0',{o.jobs.ExitCode});
            states = {o.jobs.State};
            for i=1:length(failedStates)
                allFailures = allFailures | strncmpi(failedStates{i},states,length(failedStates{i}));
            end
            
            jobNames = {o.jobs.JobName};
            state = double(allFailures);
            inProgress = ~cellfun(@isempty,regexp(states,'ING\>'));
            state(inProgress) = -2;
            uJobNames= unique(jobNames);
            for u=1:numel(uJobNames)
                thisJob = strcmpi(uJobNames{u},jobNames);
                if sum(thisJob)>1
                    % tried more than once.
                    thisFails = allFailures(thisJob);
                    nrFails = cumsum(thisFails);
                    if thisFails(end)==0 % Last was not a failure
                        state(thisJob & allFailures)    = -1;
                        state(thisJob(end))             = 0;
                    else
                        state(thisJob)                  = nrFails;
                    end
                end
            end
        end
    end
    
    %% Public Methods
    methods (Access=public)              
        function o = slurm
            % Constructor. Does not do anything except checking that shh2 is installed.
            if isempty(which('ssh2'))
                error('Please install SSH2 from the FileExchange on Matlab Central (or add it to your path)');
            end
            warning backtrace off
        end
        
        
        function delete(o)
            % Destructor. Makes sure we close the SSH connection
            close(o);            
        end
        
        
        function close(o)
            % Close the SSH connection to the cluster
            if ~isempty(o.ssh)
                o.ssh = ssh2_close(o.ssh);
            end
        end
        
        function gitpull(o,d)
            % Convenience function to pull a remote git repo from the
            % origin (typially used to sync local code development with
            % remote execution).
            if ischar(d)
                d= {d};
            end
            for i=1:numel(d)
                result = o.command(['cd ' d{i} '; git pull origin']);
                result = unique(result);
                for j=1:numel(result)
                    if isempty(result{j})
                        warning(['Nothing to update (' d{i} ')']);
                    else
                        warning(['Remote Git update ( ' d{i} '): ' result{j} ]);
                    end
                end
            end
        end
        
        function results = command(o,cmd)
            % Execute an arbitrary UNIX command on the cluster.
            if ~isempty(o.ssh)
                [o.ssh,results] = ssh2_command(o.ssh,cmd);
            else
                warning('Not connected...')
                results = {''};
            end
        end
        
        function yesno = exist(o,file,mode)
            % Check whether a file or directory exists on the cluster.
            if nargin<3
                mode ='file';
            end
            switch upper(mode)
                case 'FILE'
                    op= 'e';
                case 'DIR'
                    op = 'd';
                otherwise
                    error(['Unknown mode ' mode ]);
            end
            result = o.command([ '[ -' op ' '  file ' ] && echo "yes"']);
            yesno = strcmpi(result,'yes');
        end
        
        function result = put(o,filename, remoteDir)
            % Copy a file to the specified remore directory.
            % The remote directory is created if it does not already exist.
            % filename = The full path to the file to be copied
            % remoteDir = the name of the remote directory [o.remoteStorage]
            [localPath,filename,ext] = fileparts(filename);
            filename = [filename ext];
            if nargin<3
                remoteDir = o.remoteStorage;
            end
            if ~o.exist(remoteDir,'dir')
                result = o.command(['mkdir ' remoteDir]);
            end
            o.ssh = scp_put(o.ssh,filename,remoteDir,localPath,filename);
        end
        
        
        function connect(o)
            % Connect to the remote HPC cluster
            if ~exist(o.keyfile,'file')
                error(['The specified SSH key file does not exist: ' o.keyfile]);
            end
            o.ssh = ssh2_config_publickey(o.host,o.user,o.keyfile,' ');
            if ~o.exist(o.remoteStorage,'dir')
                result = o.command(['mkdir '  o.remoteStorage]); %#ok<NASGU>
            end
            if ~exist(o.localStorage,'dir')
                mkdir(o.localStorage);
            end
            %o.ssh.autoreconnect = true; % Seems to make connection slower?
        end
        
        function results = smap(o,options)
            % Read the current usage status of all nodes on the cluster
            % (using smap). Command line options to smap can be passed as
            % an options string.
            % See https://slurm.schedmd.com/smap.html
            % EXAMPLE:
            % o.smap  ; % Show current usage
            % o.smap('-D s')  ; Show a list of partitions            

            if nargin <2
                options ='';
            end
            uid = datestr(now,'FFF');
            filename = ['smap.output.' uid '.txt'];
            results = o.command([' smap ' options ' -c > ' o.remoteStorage '/' filename]);
            try
                o.ssh = scp_get(o.ssh,filename,o.localStorage,o.remoteStorage);            
                o.command(['rm ' o.remoteStorage '/' filename]);
            catch 
                disp('Failed to perform an smap');
            end
            localFile = fullfile(o.localStorage,filename);            
            edit(localFile);
            delete(localFile);
        end
        
        function results = retrieve(o,tag,varargin)
            % Retrieve the results generated by feval from the cluster
            % The output argument is a cell array with the output of
            % the users' mfile. Jobs that failed will have an empty element
            % in the array. The order of results is the same as the order
            % of the data provided to the mfile in the call to slurm.feval.
            %
            % tag = the tag (unique id) that identifies the job (returned
            % by slurm.feval)
            % 
            % Pressing the 'g' key in the slurmGUI calls this function
            %
            % Parm/value
            %  'partial'  - set to true to retrieve results even if some
            %  jobs are still running. [true]
            % 'deleteRemote'  - delete the data on the remote cluster after
            % copying to the localStorage [true]
            % 'deleteLocal'   - delete the data in the localStorage folder
            % (i.e. the only copy og the data is the result output of this
            % function). [true]
            % 'checkSacct' - Check the accounting log on SLURM to get
            % information on completed/failed jobs.
            
            p=inputParser;
            p.addParameter('partial',true); % Allow partial result retrieval.
            p.addParameter('deleteRemote',true); % Remove the results from the cluster (after copying)
            p.addParameter('deleteLocal',true); % Remove the results from the local disk storage (after returning to the caller).
            p.addParameter('checkSacct',true);
            p.parse(varargin{:});
            results ={};
            if p.Results.checkSacct
                [sacctData] = o.sacct('format','jobName'); % Update accounting
                state = o.failState; %0 = success, -1 = success after failing, -2 = running, >0= number of attempts.
                thisJob = strcmpi(tag,{sacctData.JobName});
                nrRunning = sum(state(thisJob)==-2);
                nrFailed  = sum(state(thisJob)>0);
                nrSuccess  =sum(ismember(state(thisJob),[0 -1]));
                
                if nrFailed >0
                    warning([num2str(nrFailed) ' jobs failed']);
                end
                
                if nrRunning >0
                    warning([num2str(nrRunning) ' jobs still running']);
                end
                if nrSuccess >0
                    warning ([num2str(nrSuccess) ' jobs completed sucessfully']);
                end
                
                if ~p.Results.partial && nrRunning >0
                    warning([num2str(nrRunning) ' jobs are still running. Let''s wait a bit...']);
                    return
                end
            end

            %% Check what;s available remotely and transfer.
            remoteJobDir = strrep(fullfile(o.remoteStorage,tag),'\','/');
            files = o.command(['cd ' remoteJobDir '; ls *.result.mat']);
            if ~isempty(files{1})
                localJobDir  = fullfile(o.localStorage,tag);
                if ~exist(localJobDir,'dir')
                    [success,message] = mkdir(localJobDir);
                    if ~success
                        error(['Failed to create local directory to store the results: ' message]);
                    end
                end                
                disp('Starting data transfer...')
                o.ssh = scp_get(o.ssh,files,localJobDir,remoteJobDir);
                disp('Data transfer done.')
            else
                warning(['No files in job directory: ' remoteJobDir]);
                return;
            end
            
            
            %% We have the files locally. Now read them and put the results 
            % in a cell array. Job n goes into the nth element of the cell array.            
            match = regexp(files,'(?<nr>\d+)\.result\.mat','names');
            match = [match{:}];
            nr = str2num(char(match.nr)); %#ok<ST2NM>
            nrJobsFound = max(nr); % Yes max...
            results = cell(1,nrJobsFound);
            for i=1:numel(files)
                load(fullfile(localJobDir,files{i}));
                results{nr(i)} = result;
            end
            
            %% cleanup if requested
            if p.Results.deleteRemote
                for i=1:numel(files)
                    o.command(['cd ' remoteJobDir '; rm ' files{i} ]);
                end                
            end            
            if p.Results.deleteLocal
                [success,message]= rmdir(localJobDir,'s');
                if ~success
                    warning(['Could not delete the local job directory: ' localJobDir ' (' message ')']);
                end
            end
        end
        function fileInFileOut(o,fun,varargin)
            p=inputParser;
            p.addParameter('inFile','',@(x) (ischar(x)|| iscell(x)));
            p.addParameter('inPath','',@(x) (ischar(x)|| iscell(x)));
            p.addParameter('outFile','',@(x) (ischar(x)|| iscell(x)));
            p.addParameter('outPath','',@(x) (ischar(x)|| iscell(x)));                        
            p.parse(varargin{:});
            
            
            
            
        end
        function jobName = feval(o,fun,data,varargin)
            % Evaluate the function fun on each of the rows of the
            % array data.
            %
            % The mfile fun should be a function that takes each column of the 
            % the data array as an input, plus optionally any
            % parameter/value pairs specified in the call to feval. (I.e.
            % it could use the inputParser object, with StructExpand=true,
            % to interpret all but its first input argument).
            %
            % The following parm/value pairs control SLURM scheduling and
            % are not passed to the function
            % 'batchOptions' - passed to slurm the same way as in
            %                   slurm.sbatch
            % 'runOptions'  = also passed to slurm. See slurm.sbatch
            %
            % 
            % 'copy' set to true to copy the mfile (fun) to the server.
            % Useful if the mfile you're executing is self-contained and
            % does not yet exist on the server. Note that it will be copied
            % to the .remoteStorage location, which is the working
            % directory of the jobs.            
            %
            % To retrieve the output each of the evaluations of fun, this
            % funciton returns a unique 'tag' that can be passed to
            % slurm.retrieve().
            %
            %
            % EXAMPLE
            % o.feval('rand',1:10) % Calls rand(1) on one worker, rand(2)
            % on another, etc. until rand(10) on the 100th worker.
            % o.feval('myfun',data,'mode','fast')
            % Will call myfun(data(1),'mode','fast')) on one worker,
            % and myfun(data(2),'mode','fast') on another.
            %
            % EXAMPLE 
            % The pctdemo_task_blackjack function takes two input
            % arguments, the number of hands to play, and how often to
            % repeat this. To play 100 hands 1000 times on 5 nodes, use:
            %  data = repmat([100 1000],[5 1]);
            % Each worker will receive one row of this matrix as the input
            % to the blackjack function. The item in the first column as the first input
            % argument, the item in the second column as the second input argument.
            % tag = o.feval('pctdemo_task_blackjack',data,'copy',true)
            % results  = o.retrieve(tag);  % Once it is done.
            
            
            % Name the job after the current time. Assuming this will be
            % unique.
            uid = datestr(now,'yy.mm.dd_HH.MM.SS.FFF');
            if ~ischar(fun)
                error('The fun argument must be the name of an m-file');
            end
            %% Prepare job directories with data and args
            if ischar(data)
                % Assume that a single string input means pass this string
                % to a single worker (and not pass each character to a
                % separate worker).
                data = {data};
            end
            
            if isrow(data)
                % This is interpreted as a column.
                data = data';
            end
            
            if isnumeric(data)
                %Make the data into a cell.
                data =num2cell(data);
            end
            
            nrDataJobs = size(data,1);
            jobName = [fun '-' uid];
            jobDir = strrep(fullfile(o.remoteStorage,jobName),'\','/');
            % Create a (unique) directory on the head node to store data and results.
            if ~o.exist(jobDir,'dir')
                result = o.command(['mkdir ' jobDir]); %#ok<NASGU>
            end
            % Save a local copy of the input data
            localDataFile = fullfile(o.localStorage,[uid '_data.mat']);
            remoteDataFile = strrep(fullfile(jobDir,[uid '_data.mat']),'\','/');
            save(localDataFile,'data','-v7.3'); % 7.3 needed to allow partial loading of the data in each worker.
            % Copy the data file to the cluster
            o.put(localDataFile,jobDir);
            
            p=inputParser;
            p.addParameter('batchOptions',{});
            p.addParameter('runOptions','');
            p.addParameter('debug',false);
            p.addParameter('copy',false);            
            p.KeepUnmatched = true;
            p.parse(varargin{:});
            
            if ~isempty(fieldnames(p.Unmatched))
                args = p.Unmatched; %#ok<NASGU>
                argsFile = [uid '_args.mat'];
                % Save a local copy of the args
                localArgsFile = fullfile(o.localStorage,argsFile);
                remoteArgsFile = strrep(fullfile(jobDir,argsFile),'\','/');
                save(localArgsFile,'args');
                % Copy the args file to the cluster
                o.put(localArgsFile,jobDir);
            else
                remoteArgsFile ='';
            end
            
            if p.Results.copy
                % Copy the mfile to remote storage. 
                mfilename = which(fun);
                o.put(mfilename,o.remoteStorage);
            end
            
            %% Start the jobs
            o.sbatch('jobName',jobName,'uniqueID','auto','batchOptions',p.Results.batchOptions,'mfile','slurm.fevalRun','mfileExtraInput',{'dataFile',remoteDataFile,'argsFile',remoteArgsFile,'mFile',fun,'nodeTempDir',o.nodeTempDir,'jobDir',jobDir},'debug',p.Results.debug,'runOptions',p.Results.runOptions,'nrInArray',nrDataJobs,'taskNr',1);            
            
        end
        
        
        function [jobId,result] = sbatch(o,varargin)
            % Interface to the sbatch command on the cluster.
            % The batch file for SLURM is stored in the local
            % storage, copied to the cluster, and then queued.
            %
            % All inputs are parameter value pairs:
            % jobName = Short name for this job
            % mfile   = The mfile (function) that each matlab session should run.
            % This (user-supplied) function should take at least two input arguments, the first is the JobID
            % that SLURM assigns. For normal jobs, the second is the taskNr
            % that the user specifies in the call to this sbatch fubction.
            % mfileExtraInput = The third and later input arguments to the mfile
            % command.
            % batchOptions =  a cell array with parm./value pairs that will
            %                   be used  in the batch file as follows:
            %                           #SBATCH= --parm=value
            %                   See the man pages for sbatch on the SLURM
            %                   Cluster for options.
            %
            % runOptions  = options for the srun command in the batch file.
            % nrInArray = This function can start Array Jobs on Slurm. This parm
            % specifies the number of matlab sessions that should be started
            % with the same mfile. If you use array jobs, your mfile will be called
            % with the array number as the second argument (and any extra
            % inputs specified by mfileExtraInput).
            % debug = true/false.  Set to true to inspect the sbatch file
            % that would be run on the cluster. Nothing is actually run.
            %
            % 'retryBatchFile' - A batch file that will be queued on SLURM
            % without further processing. This is used by the slurm.retry
            % function.
            % 'uniqueID' - Use this to generate unique jobnames for each
            % job. If the specified jobName is already unique, set this to
            % '', if you want this function to make the jobName unique,
            % specify 'auto'.
            % 'taskNr' - Used only for standard (non-array) jobs: this is
            % the number that will be passed to your mfile as the second
            % argument.
            %
            % OUTPUT
            % jobId  = The SLURM JobId (if successful)
            % result = Output of sbatch on the cluster
            %
            % EXAMPLE
            % o.sbatch('jobName','sort','mfile','psort','mfileExtraInput','parmFile','nrInArray',10);
            %
            % This would start Matlab on 10 CPU's. In each Matlab session the psort.m mfile would be started as follows:
            % psort(JOBID, 1,'parmFile') (on the first CPU)
            % psort(JOBID, 2,'parmFile') (on the second CPU),etc.
            %
            % The parmFile could be a file that you previously sent to the
            % cluster (using o.put) and inside the psort you could then
            % load this file and use the first argument to pull some subset
            % of parameters  from the file that should be used for a particular
            % matlab session on the cluster.
            % 
            % See also slurm/feval for an example function that uses
            % slurm/sbatch to submit jobs to the scheduler.
            
            
            
            p = inputParser;
            p.addParameter('jobName','job',@ischar);
            p.addParameter('batchOptions',{},@iscell);
            p.addParameter('runOptions','',@ischar);
            p.addParameter('mfile','',@ischar);
            p.addParameter('mfileExtraInput',{},@iscell); % pv pairs
            p.addParameter('command','',@ischar);
            p.addParameter('nrInArray',1,@(x) (isnumeric(x) && isscalar(x)));
            p.addParameter('debug',false,@islogical);
            p.addParameter('retryBatchFile','',@ischar);
            p.addParameter('uniqueID','',@ischar); % Use 'auto' to generate
            p.addParameter('taskNr',0,@isnumeric);
            p.parse(varargin{:});
            
            if isempty(p.Results.retryBatchFile)
                %Construct a new batch file for sbatch
                if ~isempty(p.Results.mfile)
                    % Wrap an mfile
                    extraIn = '';
                    for i=1:2:length(p.Results.mfileExtraInput)
                        if ischar(p.Results.mfileExtraInput{i+1})
                            strValue = ['''' p.Results.mfileExtraInput{i+1} ''''];
                        elseif isnumeric(p.Results.mfileExtraInput{i+1})
                            strValue = ['[' num2str(p.Results.mfileExtraInput{i+1}) ']'];
                        else
                            error('Extra input to the mfile must be parm/value pairs. Value can only be numeric or string');
                        end
                        extraIn = cat(2,extraIn,',''', p.Results.mfileExtraInput{i},''',',strValue);
                    end
                    if p.Results.nrInArray>1
                        runStr = 'matlab -nodisplay -nodesktop -r "try;cd ''%s'';%s($SLURM_JOB_ID,$SLURM_ARRAY_TASK_ID %s);catch;lasterr,exit(1);end;exit(0);"';
                        run = sprintf(runStr,o.remoteStorage,p.Results.mfile,extraIn);
                    else
                        runStr = 'matlab -nodisplay -nodesktop -r "try;cd ''%s''; %s($SLURM_JOB_ID,%d %s);catch;lasterr,exit(1);end;exit(0);"';
                        run = sprintf(runStr,o.remoteStorage,p.Results.mfile,p.Results.taskNr,extraIn);
                    end
                elseif ~isempty(p.Results.command)
                    % The user knows what to do. Run this command as is with srun.
                    run = p.Results.command;
                else
                    error('command and run cannot both be empty...');
                end
                
                
                jobName = p.Results.jobName;
                if p.Results.nrInArray>1
                    batchOptions = cat(2,p.Results.batchOptions,{'array',['1-' num2str(p.Results.nrInArray)]});
                    outFile = '%A_%a.out';
                else
                    batchOptions = p.Results.batchOptions;
                    outFile = '%A.out';
                end
                % Options with empty values are removed.
                empty = find(cellfun(@isempty,batchOptions(2:2:end)));
                batchOptions([2*empty-1 2*empty])= [];
                if isempty(p.Results.uniqueID)
                    % Presumable the user knows what they're doing and the
                    % jobName is unique on its own
                    uniqueID = '';
                elseif strcmpi(p.Results.uniqueID,'auto')
                    %Generate a unique ID
                    [~,uniqueID]=fileparts(tempname);
                    uniqueID = ['.' uniqueID];
                else
                    uniqueID = ['.' p.Results.uniqueID];
                end
                batchFile = sprintf('%s%s.sh',jobName,uniqueID);
                fid  =fopen( fullfile(o.localStorage,batchFile) ,'w'); % no t (unix line endings are needed)
                fprintf(fid,'#!/bin/bash\n');
                
                batchOptions = cat(2,batchOptions, {'job-name',jobName,'output',outFile,'error',outFile,'comment',slurm.encodeComment('','sbatch',batchFile)});
                %   batchOptions = cat(2,batchOptions, {'job-name',jobName,'comment',slurm.encodeComment('','sbatch',batchFile)});
                for opt=1:2:numel(batchOptions)
                    if isempty(batchOptions{opt+1})
                        fprintf(fid,'#SBATCH --%s\n',batchOptions{opt});
                    elseif isnumeric(batchOptions{opt+1})
                        fprintf(fid,'#SBATCH --%s=%d\n',batchOptions{opt},batchOptions{opt+1});
                    elseif ischar(batchOptions{opt+1})
                        isSpace  = batchOptions{opt+1}==' ';
                        if any(isSpace)
                            batchOptions{opt+1}(isSpace) = '';
                            warning(['Removing spaces from Slurm Batch Option ''' batchOptions{opt} ''' now set to:''' batchOptions{opt+1} '''']);
                        end
                        fprintf(fid,'#SBATCH --%s=%s\n',batchOptions{opt},batchOptions{opt+1});
                    else
                        error('sbatch options should be char or numeric');
                    end
                end
                fprintf(fid,'srun %s %s\n',p.Results.runOptions,run);
                fclose(fid);
            else
                % Use the specified batch file. Extract the job name.
                batchFile = p.Results.retryBatchFile;
                jobName   = strsplit(batchFile,'.');
                jobName = jobName{1};
            end
            
            if p.Results.debug
                warning ('Debug mode. Nothing will be submitted to SLURM')
                edit (fullfile(o.localStorage,batchFile));
                jobId=0;result = 'debug mode';
            else
                if isempty(p.Results.retryBatchFile)
                    % Copy the flie to the cluster
                    o.put(fullfile(o.localStorage,batchFile),o.remoteStorage);
                end
                % Start the sbatch
                result = o.command(sprintf('cd %s ;sbatch %s/%s',o.remoteStorage,o.remoteStorage,batchFile));
                jobId = str2double(regexp(result{1},'\d+','match'));
                if isempty(jobId) || isnan(jobId)
                    warning(['Failed to submit ' jobName ' (Msg=' result{1} ')']);
                else
                    warning(['Successfully submitted ' jobName ' (JobID=' num2str(jobId) ')']);
                end
            end
        end
        
        function result = cancel(o,varargin)
            % Cancel a slurm job by its Job ID
            p=inputParser;
            p.addParameter('jobId',[],@(x)(ischar(x) || iscell(x)));
            p.parse(varargin{:});
            
            if ischar(p.Results.jobId)
                jobIds = {p.Results.jobId};
            else
                jobIds = p.Results.jobId;
            end
            cmd = ['scancel ' sprintf('%s ',jobIds{:})];
            result = o.command(cmd);
            %warning(result{1})
        end
        
        
        function [list] = retry(o,varargin)
            % Check the slurm acct log and retry all jobs that failed.
            % 'starttime'  - retry only jobs submitted after this time
            % 'endtime' - retry only jobs submitted before this time
            % 'list'  - show and return a list only.
            % 'maxNrFails' - Do not attempt to retry jobs that failed this
            % number of times.
            % OUTPUT
            % failedJobs = A list of jobNames that failed (cell)
            % giveUpIx  =  An logical index into failedJobs with the jobs
            % that failed more often than maxNrFails
            % msg = cell array of messages about the retry.
            % 'callSacct' = set to true to force rereading the sacct log.
            % [true]
            %
            p=inputParser;
            p.addParameter('list',false,@islogical);
            p.addParameter('maxNrFails',10,@isnumeric);
            p.addParameter('selection',[],@isnumeric); %
            p.addParameter('jobId',[],@(x)(ischar(x) || iscell(x)));
            p.parse(varargin{:});
            
            if o.nrJobs==0
                list = {};
                warning('No jobs found')
            else
                if isempty(p.Results.jobId)
                    state = o.failState;
                    if isempty(p.Results.selection)
                        jobIx    = find(state > 0 & state < p.Results.maxNrFails);
                    else
                        jobIx    = find(state > 0 & state < p.Results.maxNrFails & ismember(1:o.nrJobs,p.Results.selection));
                    end
                else
                    jobIx = find(ismember({o.jobs.JobID},p.Results.jobId));
                end
                list = {o.jobs(jobIx).JobName};
                
                if ~p.Results.list
                    alreadyRetried = {};
                    for j=jobIx
                        batchFile = slurm.decodeComment(o.jobs(j).Comment,'sbatch');
                        if length(batchFile)~=1 || isempty(batchFile{1})
                            error('The comment field should contain the batch file...');
                        else
                            batchFile = batchFile{1};
                        end
                        if ~ismember(batchFile,alreadyRetried)
                            o.sbatch('retryBatchFile',batchFile);
                            alreadyRetried = cat(2,alreadyRetried,batchFile);
                        end
                    end
                end
            end
        end
        
        
        function [groups,groupIx,subs,jobIx]=jobGroups(o,expression)
            % Often jobs belong together in a group. By using convention 
            % jobName =  Job-SubJob, the 'group' (job) and its elements (subjob)
            % can be determined from the jobName. This is used by slurmGui to make the
            % interaction with the logs less cluttered.
            if nargin<2
                % Default format  GROUP-ESUB
                expression = '(?<group>[\w_-]*)-(?<sub>[\w\d\.]*$)';
            end
            groups ={};groupIx = [];subs={};jobIx ={};
            if o.nrJobs >0
                jobNames = {o.jobs.JobName};
                match = regexp(jobNames,expression,'names');
                noGroupMatch = cellfun(@isempty,match);
                [match{noGroupMatch}]= deal(struct('group','Not Grouped','sub',''));
                match = cat(1,match{:});
                subs = {match.sub};
                subs(noGroupMatch)= jobNames(noGroupMatch);
                [jobNames{noGroupMatch}] = deal('Not Grouped');
                [groups,~,groupIx] = unique({match.group});
                nrGroups = length(groups);
                jobIx = cell(1,nrGroups);
                for i=1:nrGroups
                    jobIx{i} = find(strncmp(groups{i},jobNames,length(groups{i})));
                end
            end
        end
        
        
        function logFile(o,jobId,varargin )
            % Retrieve a log file from the cluster. This can be the redirected
            % stdout (type =out) or stderr (type=err). (Although currently
            % sbatch writes both out and err to the same .out file so the
            % 'out' logfile has all the information.       
            % Input
            % jobId  = jobID
            % 'element' =  element of a job array (ignored for non-array batch
            % jobs).
            % 'type' = 'out' or 'err'
            % 'forceRemote' = Force reading from remote [true].
            % OUTPUT
            % File is opened in editor
            %
            p =inputParser;
            p.addParameter('element',[],@isnumeric);% Element of array job
            p.addParameter('forceRemote',true,@islogical);% force reading from remote storage
            p.addParameter('type','out',@(x) (ischar(x) && ismember(x,{'out','err','sh'})));
            p.parse(varargin{:});
            
            %% Construct the file name
            [tf,ix] = ismember(jobId,{o.jobs.JobID});
            if any(tf)
                if strcmpi(p.Results.type,'SH')
                    filename = slurm.decodeComment(o.jobs(ix).Comment,'sbatch');
                    if length(filename)~=1 || isempty(filename{1})
                        error('The comment field should contain the batch file...');
                    else
                        filename = filename{1};
                    end
                else
                    filename = [ o.jobs(ix).JobID '.' p.Results.type];
                end
            else
                error(['No matching jobId found (' jobId ')']);
            end
            %% Retrieve it from the cluster if not available local
            localName= fullfile(o.localStorage,filename );
            if ~exist(localName,'file') || p.Results.forceRemote
                remoteName= strrep(fullfile(o.remoteStorage,filename ),'\','/');
                if o.exist(remoteName,'file')
                    o.ssh = scp_get(o.ssh,filename,o.localStorage,o.remoteStorage);
                else
                    warning(['File does not exist: ' remoteName]);
                    return;
                end
            end            
            %% Open in editor
            edit(localName);            
        end
        
        
        function [data,elapsed] = sacct(o,varargin)
            % Retrieve slurm accounting data from the cluster.
            % INPUT
            % List of parameter/value pairs:
            % 'jobId' = A jobID to retrieve info for a specific job.
            %           Defaults to retrieve information for all jobs in the current jobs list.
            %           Nan to retrieve information for all jobs in sacct.
            % 'user' = Restrict to jobs started by this user [o.user]
            % 'format' = Comma separated string of information to retrieve
            %           (see man sacct , and look at the inputs for format.
            %           Defaults to a small subset of fields.
            % 'starttime' = Restrict to jobs started after this date/time.
            %               Use a datenum or a datestr. Default is 'now-1'.
            %               (i.e. all jobs started 24 hours before now)
            % 'endtime'  = Restrict to jobs that were submitted before this
            %               time.
            % 'removeSteps' = Remove the .batch and .0 jobs that sacct
            % shows (but which are really just part of the main job) [true]
            % OUTPUT
            % data = A struct with all information retrieved from SLURM.
            % elapsed = the time between start and end time.
            % If no output is requested, the jobs member variable is
            % updated with the new information
            %
            % QUIRKS
            % If you set a starttime in the future, you still get jobs that
            % are running now. This is a problem of sacct (which apparently
            % has no good way of determining the starttime until the job is
            % done).
            
            
            p = inputParser;
            p.addParameter('jobId',NaN,@isnumeric);
            p.addParameter('user',o.user,@ischar);
            p.addParameter('format','jobId,State,ExitCode,jobName,Comment,submit',@ischar);
            p.addParameter('starttime',o.from,@(x)(ischar(x) || isnumeric(x)));
            p.addParameter('endtime',o.to,@(x)(ischar(x) || isnumeric(x)));
            p.addParameter('removeSteps',true,@islogical);
            p.parse(varargin{:});
            
            if isnan(p.Results.jobId)
                jobIdStr =  '';
            else
                jobIdStr =sprintf('%d,',p.Results.jobId(:));
                jobIdStr = ['--jobs=' jobIdStr];
            end
            if isinf(p.Results.endtime)
                endTime = now+1;
            else
                endTime = max(p.Results.starttime+1,p.Results.endtime);
            end
            o.from  = p.Results.starttime;
            o.to = endTime;
            
            if ~isempty(strfind(upper(p.Results.format),'SUBMIT'))
                format = p.Results.format;
            else
                format = cat(2,p.Results.format,',Submit');
            end
            
            if isempty(strfind(upper(format),'JOBID'))
                format = cat(2,format,',jobId');
            end
            if ~isempty(p.Results.user)
                userCmd = [' --user=' p.Results.user];
            else
                userCmd = '';
            end
            
            cmd  = ['sacct  --parsable2 --format=' format '  ' jobIdStr userCmd ' --starttime=' datestr(p.Results.starttime,'yyyy-mm-ddTHH:MM:SS') ' --endtime=' datestr(endTime,'yyyy-mm-ddTHH:MM:SS')];
            results = o.command(cmd);
            if length(results)>1
                fields = strsplit(results{1},'|');
                for j=1:numel(results)-1
                    thisData = strsplit(results{j+1},'|','CollapseDelimiters',false);
                    data(j) = cell2struct(thisData,fields,2); %#ok<AGROW>
                end
                
                elapsed = datetime(datestr(slurm.matlabTime(data(end).Submit),0),'InputFormat','dd-MMM-yyyy HH:mm:SS')-datetime(datestr(slurm.matlabTime(data(1).Submit),0),'InputFormat','dd-MMM-yyyy HH:mm:SS');
                elapsed.Format = 'h';
                if p.Results.removeSteps
                    % Remove the jobs that seem to be part of the jobs that I
                    % start (they have jobIDs with .0 or .batch at the end)
                    stay = cellfun(@isempty,regexp({data.JobID},'\.0\>')) & cellfun(@isempty,regexp({data.JobID},'\.batch\>')) ;
                    data = data(stay);
                end
            else
                %warning(['No sacct information was found for ' jobIdStr '(command = ' cmd ')']);
                data =[];
                elapsed = duration(0,0,0);
                elapsed.Format = 'h';
                return
            end
            
            if nargout==0
                o.jobs = data;
            end
        end
        
        
        function jobs = jobsAtTime(o,time)
            % Find jobs that were submitted at a specific time,
            allTimes = {o.jobs.Submit};
            allTf = false(size(allTimes));
            for i=1:length(time)
                [tf] = strcmp(time{i},allTimes);
                allTf = allTf | tf;
            end
            if any(allTf)
                jobs = o.jobs(allTf);
            else
                jobs = [];
            end
        end
        
        function value = elementsForJob(o,jobName)
            % Find the elements of an array job
            [tf,ix] = ismember(jobName,{o.jobs.name});
            if any(tf)
                value = [o.jobs(ix).array];
            end
        end
    end
    
    %% Static helper function
    methods (Static)
        function T = slurmTime(t)
            % Represent a datenum in slurm format.
            T = datestr(t,'yyyy-mm-ddTHH:MM:SS');
        end
        function T= matlabTime(t)
            % Rerpresent a slurm time in Matlab's datenum format.
            if ischar(t)
                tCell= {t};
            else
                tCell =t;
            end
            nrT = numel(tCell);
            T = zeros(1,nrT);
            for i=1:nrT
                t=tCell{i};
                if length(t)==19
                    T(i) = datenum(str2double(t(1:4)),str2double(t(6:7)),str2double(t(9:10)),str2double(t(12:13)),str2double(t(15:16)),str2double(t(18:19)));
                end
            end
        end
        
        function comment =encodeComment(comment,tag,value)
            % To store information in the comment field of a job, we
            % encode it using a specific tag:value format.
            % By passing the existing comment as the first argument and
            % the additional tag/value as the second and third, multiple
            % pieces of information can be stored.
            % INPUT
            % comment  - the existing comment (can be '');
            % tag      - Name for this piece of inforamtion
            % value    -  The information to be stored.
            %OUTPUT
            % comment  - the new comment. This string can be passed to
            % slurm.sbatch  using the 'comment' parameter to be stored in
            % the sbatch file and thereby in the sacct log.
            if isempty(comment)
                comment = [tag ':' value];
            else
                comment = [comment ':' tag,':' value];
            end
        end
        function value =decodeComment(comment,tag)
            % Extract the value of a specific tag from the commetn field.
            % Assumes that the comment was encoded using encodeComment
            % INPUT
            % comment  - the entire comment (usually retrieved with sacct)
            % tag      - the specific piece of information to extract
            % OUTPUT
            % value     - the information
            
            if ischar(comment)
                comment = {comment};
            end
            value=cell(1,length(comment));
            [value{:}] = deal('');
            for i=1:length(comment)
                tmp  = strsplit(comment{i},':');
                ix = find(strcmpi(tag,tmp(1:2:end)));
                if ~isempty(ix)
                    value{i} = tmp{ix+1};
                end
            end
        end
        
        function fevalRun(jobID,taskNr,varargin) %#ok<INUSL>
            % This function runs on the cluster in response to a call to slurm.feval on the client.
            % It is not meant to be called directly. See slurm.feval.
            %
            % It will run a specified mfile on an element of the specified data array
            % INPUT: 
            % jobID = slurm job id
            % taskNr = The number of this job in the array of jobs. This
            % number is used to pick one item from the data array.
            % 
            % Other properties are specified as parm/value pairs
            % 
            % 'mfile'  - The Mfile that will be run,.
            % 'datafile'  - File containing the that will be passed to
            % mfile.
            % 'argsFile'  - File containing the extra input arguments for
            % the mfile
            % 'nodeTempDir' - folder on the nodes where temporary
            % information can be saved (e.g. /scratch)
            % 'jobDir' - directory on the head node where the results are
            % stored. (and later retrieved by slurm.retrieve)
            %
            p = inputParser;
            p.addParameter('dataFile','');
            p.addParameter('argsFile','');
            p.addParameter('mFile','');
            p.addParameter('nodeTempDir','');
            p.addParameter('jobDir','')
            p.parse(varargin{:});
            
            dataMatFile = matfile(p.Results.dataFile); % Matfile object for the data so that we can pass a slice
            if ~isempty(p.Results.argsFile)  % args may have extra inputs for the user mfile
                load(p.Results.argsFile);
            else
                args = {};
            end
            % Load a single element of the data cell array from the matfile and
            % pass it to the mfile, together with all the args. 
                     
            data = dataMatFile.data(taskNr,:); % Read a row from the cell array
            result = feval(p.Results.mFile,data{:},args{:}); % Pass all cells of the row to the mfile as argument (plus optional args)
            
            % Save the result in the jobDir as 1.result.mat, 2.result.mat
            % etc.
            slurm.saveResult([num2str(taskNr) '.result.mat'] ,result,p.Results.nodeTempDir,p.Results.jobDir);
        end
        
        function saveResult(filename,result,tempDir,jobDir) %#ok<INUSL>
            % Save a result first on a tempDir on the node, then copy it to
            % the head node, using scp. Used by slurm.run
            
            fName =fullfile(tempDir,filename);
            save(fName,'result');
            % Copy to head
            scpCommand = ['scp ' fName ' ' jobDir];
            tic;
            warning(['Transferring data back to head node: ' scpCommand]);
            [scpStatus,scpResult]  = system(scpCommand);
            if iscell(scpResult) ;scpResult = char(scpResult{:});end
            
            % Warn about scp errors
            if scpStatus~=0
                scpResult %#ok<NOPRT>
                warning(['SCP of ' fName ' failed' ]); % Signal error
            elseif ~isempty(scpResult)
                warning(scpResult)
            end
            
            [status,result]  = system(['rm  ' fName]);
            if status~=0
                warning(['rm  of ' fName ' failed'  ]); % Warning only
                result %#ok<NOPRT>
            end
            
        end
    end
end



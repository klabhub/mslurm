% A class to interact with a remote SLURM cluster using SSH
%
% This allows you to submit Matlab jobs to a SLURM scheduler, without using
% the Matlab Parallel Server, by simply starting as many Matlab sessions as
% you request.
%
% See README.md for installation and usage instructions.
%
% Key functions for the user are
%   batch() - Run a script (or function with some parm/value pairs) N times in parallel.
%   feval()  - Analyze each of the rows of an array of data with the same function.
%   fileInFileOut() - Given a list of files, analyze each file.
%   taskBatch() - Similar to feval() but with options to
%                       retry/continue/reuse previously uploaded data.
%
% See also mslurmApp
%
% cluster.
%
% BK - Jan 2015
% June 2017 - Public release.
% Sept 2023 - Update.

%#ok<*ISCLSTR>
classdef mslurm < handle

    properties (Constant)
        PREFGRP = "mSlurm"  % Group that stores string preferences for slurm
        PREFS   = ["user","keyfile","host","remoteStorage","localStorage", "matlabRoot","nodeTempDir","headRootDir","mslurmFolder"];
        SBATCHFILE = 'mslurmSBatch.sh';
        STDOUTFILE = 'stdout';
        STDERRFILE = 'stderr';
    end

    properties (SetAccess=public, GetAccess=public)
        %% Installation defaults (read from preferences at construction)
        remoteStorage       = ''; % Location on HPC Cluster where scripts and logs will be written
        localStorage        = ''; % Location on client where logs and scripts will be written
        host                = ''; % Host Address
        user                = ''; % Remote user name
        keyfile             = ''; % Name of the SSH key file. (full name)
        matlabRoot          = ''; % Matlab root on the cluster.
        mslurmFolder        = ''; % Folder where this mslurm toolbox is installed on the cluster.

        %% Session defaults (can be overruled when submitting specific jobs).
        nodeTempDir         = '';  % The path to a directory on a node that can be used to save data temporarily (e.g. /scratch/)
        headRootDir         = '';  % The path to the directory on the head node where results can be copied
        startupDirectory    = '';  % The directory where matlab will start (-sd command line argument)
        workingDirectory    = ''; % The directory where the code will execute (if unspecified, defaults to remoteStorage location)
        addPath             = {}; % Cell array of folders that shoudl be added to the path.
        batchOptions        = {}; % Options passed to sbatch (parm,value pairs)
        runOptions          = ''; % Options passed to srun

    end

    properties (Dependent,SetAccess=protected)
        pwd;                    % Working directory on the SLURM cluster
        nrJobs;                 % Number of jobs currently available in the log (i.e. submitted between .from and .to)
        isConnected;            % Check that we have an open SSH connection to the cluster.
        failState;              % Current state of each of the jobs in the log.
        failStateName;
        uid;                    % A unique ID.
    end
    properties (SetAccess=protected, GetAccess=public)
        jobs;                    % A structure with the information retrieved from the slurm accounting logs (see the sacct function below)
    end

    properties (SetAccess=protected, GetAccess=public, Transient)
        ssh;             % An SSH2 structure
        maxArraySize;    % Read from the SLURM
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
            v = ~isempty(o.ssh) && ~isempty(o.ssh.connection);
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
            states = {o.jobs.State};
            jobNames = {o.jobs.JobName};
            allFailures = ~strcmpi('0:0',{o.jobs.ExitCode}) | ismember(states,failedStates);

            state = double(allFailures);
            inProgress = ~cellfun(@isempty,regexp(states,'ING\>'));
            state(inProgress) = -2;
            uJobNames= unique(jobNames);
            for u=1:numel(uJobNames)
                thisJob = strcmpi(uJobNames{u},jobNames);
                if sum(thisJob)>1
                    % tried more than once.
                    thisFails = allFailures(thisJob);
                    nrFails = sum(thisFails);
                    if thisFails(end)==0 % Last was not a failure
                        state(thisJob & allFailures)    = -1;
                        state(thisJob(end))             = 0;
                    else
                        state(thisJob)                  = nrFails;
                    end
                end
            end
        end


        function stateNames = get.failStateName(o)
            state = o.failState;
            % Map to a name
            stateNames = cell(size(state));
            [stateNames{state==0}] = deal('SUCCESS'); % (completed success)
            [stateNames{state==1}] = deal('RETRIED');  % Blue Failed but retried
            [stateNames{state==-2}] =deal('RUNNING');
            [stateNames{state>0 & state<=10}] =deal('FAILED');
            [stateNames{state>10}] =deal('FREQFAIL');
        end

        function v = get.uid(o)
             v = char(datetime("now",'Format','yy.MM.dd_HH.mm.SS.sss'));
        end
    end

    %% Public Methods
    methods (Access=public)
        function o = mslurm(pv)
            % Construct a mslurm object.Specify parameter/value pairs for
            % Host, User, keyfile, remoteStorage, localStorage, and
            % matlabRoot. Parameters that are not specified use the
            % preferences stored per machine (see mslurm.install)
            arguments
                pv.host (1,1) string = mslurm.getpref('host');
                pv.user (1,1) string = mslurm.getpref('user');
                pv.keyfile (1,1) string =mslurm.getpref('keyfile');
                pv.remoteStorage (1,1) string = mslurm.getpref('remoteStorage');
                pv.matlabRoot (1,1) string = mslurm.getpref('matlabRoot');
                pv.localStorage (1,1) string = mslurm.getpref('localStorage');
                pv.nodeTempDir (1,1) string = mslurm.getpref('nodeTempDir');
                pv.headRootDir (1,1) string = mslurm.getpref('headRootDir');
                pv.mslurmFolder (1,1) string = mslurm.getpref('mslurmFolder');
            end
            % Constructor.

            % Add the SSH2 submodule to the search path (skipped if the user
            % already has ssh2 on the path.)
            if isempty(which('ssh2'))
                here =fileparts(mfilename('fullpath'));
                addpath(fullfile(here,'matlab-ssh2','ssh2'));
            end
            ssh2Install = fileparts(which('ssh2'));
            jarName = strrep(fullfile(ssh2Install,'ganymed-ssh2-build250','ganymed-ssh2-build250.jar'),'\','/');
            if ~exist(jarName,'file')
                error('Could not load the SSH2 java file. Please check your matlab-ssh2 installation.')
            end
            javaaddpath(jarName);
            % Setup the object with saved prefs, overruled by session specific
            % input arguments to the constructor
            o.host= char(pv.host);
            o.user = char(pv.user);
            o.keyfile = char(pv.keyfile);
            o.remoteStorage = char(pv.remoteStorage);
            o.matlabRoot = char(pv.matlabRoot);
            o.localStorage = char(pv.localStorage);
            o.nodeTempDir  = char(pv.nodeTempDir);
            o.headRootDir = char(pv.headRootDir);
            o.mslurmFolder= char(pv.mslurmFolder);
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

        function v = fairshare(o,usr)
            % Show the fairsahre score of the uer (or any user)
            % 1= good, 0= bad.
            if nargin <2
                usr = o.user;
            end
            v = o.command(['sshare --noheader --Users ' usr ' --format="FairShare"']);
            v= v{1}; % First line is user, second is 'general'?
            v = str2double(v);
        end

        function gitpull(o,folders,pv)
            % Convenience function to pull a remote git repo from the
            % origin (typially used to sync local code development with
            % remote execution). Note that submodules will alsobe updated
            % using submodule update --recursive.
            %
            % INPUT
            % folders  - The folder with a git repo on the cluster. Or a cell
            % array with such folder names.
            % 'submodules' [false] Set this to true to update submodules recursively with git submodule update --recursive
            %                   Note that in this case the folders have to
            %                   be the toplevel of the git repo.
            %
            arguments
                o (1,1) mslurm
                folders (1,:)
                pv.submodules (1,1) logical = false
            end

            if ischar(folders)
                folders= {folders};
            end
            for i=1:numel(folders)
                if pv.submodules
                    % This sumodule update will only work if the folder is the root folder
                    % of the git repo.
                    cmd =sprintf('cwd= ${pwd} && cd %s && git pull origin && git submodule update --recursive && cd $cwd',folders{1});
                else
                    % This will work from anywhere in the repo
                    cmd =sprintf('cwd= ${pwd} && cd %s && git pull origin && cd $cwd',folders{1});
                end
                result = o.command(cmd);
                mslurm.log('%s - %s',folders{i}, strjoin(result,'\n'))
            end

        end

        function [results,err] = command(o,cmd,varargin)
            if nargin>2
                cmd = sprintf(cmd,varargin{:});
            end
            % Execute an arbitrary UNIX command on the cluster.
            if ~isempty(o.ssh)
                USESSHERR = nargout('ssh2_command')==3; % If you have the klab SSH fork it returns err messages for debugging. Otherwise keep this as false.
                try
                    if USESSHERR
                        [o.ssh,results,err] = ssh2_command(o.ssh,cmd);
                        if ~isempty(err{1})
                            fprintf('The %s command generated the following errors:\n %s',cmd,strjoin(err,'\n'));
                        end
                    else
                        err= {'unknonwn error'};
                        [o.ssh,results] = ssh2_command(o.ssh,cmd);
                    end
                catch me
                    if any(ismember({me.stack.name},'mslurm.connect'))
                        % Avoid recursion.
                        rethrow(me)
                    else
                        % Try to reconnect once.
                        mslurm.log('SSH error. Trying to reconnect');
                        connect(o);
                        [o.ssh,results] = ssh2_command(o.ssh,cmd);
                        mslurm.log('Reconnected.')
                    end
                end

            else
                mslurm.log('Not connected...')
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

            if ~exist(filename,'file')
                error('Local file %s not found. Cannot copy to cluster.',filename)
            end
            [localPath,filename,ext] = fileparts(filename);
            filename = [filename ext];
            if nargin<3
                remoteDir = o.remoteStorage;
            end
            if ~o.exist(remoteDir,'dir')
                result = o.command(['mkdir ' remoteDir]);
            end
            try
                o.ssh = scp_put(o.ssh,filename,remoteDir,localPath,filename);
            catch
                mslurm.log('SSH error. Trying to reconnect');
                connect(o);
                o.ssh = scp_put(o.ssh,filename,remoteDir,localPath,filename);
            end
        end

        function get(o,files,localJobDir,remoteJobDir,deleteRemote)
            if ischar(files)
                files= {files};
            end
            if nargin<5
                deleteRemote = false;
                if nargin < 4
                    remoteJobDir = o.remoteStorage;
                    if nargn <3
                        localJobDir = o.localStorage;
                    end
                end
            end
            mslurm.log('Starting data transfer...')
            try
                o.ssh = scp_get(o.ssh,files,localJobDir,remoteJobDir);
            catch
                mslurm.log('SSH error. Trying to reconnect');
                connect(o)
                o.ssh = scp_get(o.ssh,files,localJobDir,remoteJobDir);
            end
            mslurm.log('Data transfer done.')

            %% cleanup if requested
            if deleteRemote
                mslurm.log('Deleting remote files')
                o.command(['cd ' remoteJobDir ]);
                for i=1:numel(files)
                    o.command([' rm ' files{i} ]);
                end
            end

        end

        function connect(o)
            % Connect to the remote HPC cluster
            if ~exist(o.keyfile,'file')
                error(['The specified SSH key file does not exist: ' o.keyfile]);
            end
            o.ssh = ssh2_config_publickey(o.host,o.user,o.keyfile,' ');
            o.ssh.command_ignore_stderr = false;

            if ~o.exist(o.remoteStorage,'dir')
                result = o.command(['mkdir '  o.remoteStorage]); %#ok<NASGU>
            end
            if ~exist(o.localStorage,'dir')
                mkdir(o.localStorage);
            end
            %o.ssh.autoreconnect = true; % Seems to make connection slower?
            result = o.command('scontrol show config | sed -n ''/^MaxArraySize/s/.*= *//p''');
            o.maxArraySize = str2double(result{1});
        end

        function [T,msg] = smap(o,options)
            % Read the current usage status of all nodes on the cluster
            % (using smap). Command line options to smap can be passed as
            % an options string.
            % See https://slurm.schedmd.com/smap.html
            % EXAMPLE:
            % o.smap  ; % Show current usage
            % o.smap('-D s')  ; Show a list of partitions
            %
            % OUTPUT
            %  T = table with the smap info
            % msg = warning message

            if nargin <2
                options ='';
            end
            uid = char(datetime("now",'Format','sss'));
            filename = ['smap.output.' uid '.txt'];
            msg = o.command([' smap ' options ' -c > ' o.remoteStorage '/' filename]);
            try
                o.ssh = scp_get(o.ssh,filename,o.localStorage,o.remoteStorage);
                o.command(['rm ' o.remoteStorage '/' filename]);
            catch
                msg = 'Failed to perform an smap';
            end
            localFile = fullfile(o.localStorage,filename);
            if nargout ==0
                mslurm.log(msg);
                edit(localFile);
            else
                T = readtable(localFile);
            end
            delete(localFile);
        end

        function results = retrieve(o,tag,varargin)
            % Retrieve the results generated by feval or batch from the cluster
            % The output argument is a cell array with the output of
            % the users' mfile. Jobs that failed will have an empty element
            % in the array. The order of results is the same as the order
            % of the data provided to the mfile in the call to mslurm.feval.
            %
            % tag = the tag (unique id) that identifies the job (returned
            % by mslurm.feval and mslurm.batch)
            %
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
                o.sacct; % Update accounting
                state = o.failState; %0 = success, -1 = success after failing, -2 = running, >0= number of attempts.
                thisJob = strcmpi(tag,{o.jobs.JobName});
                nrRunning = sum(state(thisJob)==-2);
                nrFailed  = sum(state(thisJob)>0);
                nrSuccess  =sum(ismember(state(thisJob),[0 -1]));

                if nrFailed >0
                    mslurm.log([num2str(nrFailed) ' jobs failed']);
                end

                if nrRunning >0
                    mslurm.log([num2str(nrRunning) ' jobs still running']);
                end
                if nrSuccess >0
                    mslurm.log ([num2str(nrSuccess) ' jobs completed sucessfully']);
                end

                if ~p.Results.partial && nrRunning >0
                    mslurm.log([num2str(nrRunning) ' jobs are still running. Let''s wait a bit...']);
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
                get(o,files,localJobDir,remoteJobDir);
            else
                mslurm.log(['No files in job directory: ' remoteJobDir]);
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
                load(fullfile(localJobDir,files{i})); %#ok<LOAD>
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
                    mslurm.log(['Could not delete the local job directory: ' localJobDir ' (' message ')']);
                end
            end
        end
        function fileInFileOut(o,fun,varargin)
            % This function takes a list of files, generates one job per
            % file to process the file and then saves the results in an
            % output file.
            %
            % The first input argument 'fun' should be a function that takes a filename
            % (complete with path) as its first input, and returns a single
            % ouput. This single output will be saved to the output file.
            % Additional inputs to 'fun' can be specified as parameter/value
            % pairs.
            %
            % Input args: (parm/value pairs).
            % 'inFile' -  a cell array of file names to process
            % 'inPath'  -  a single path (that exists on the server)
            % 'outTag' - This tag will be appended to the output filename
            % 'outPath' - a single path to which the restuls will be
            % written.
            %
            % Slurm options can be specified as
            % 'batchOptions'   -see mslurm.sbatch
            % 'runOptions' - see mslurm.sbatch
            %
            % If the 'fun' is self-contained you can copy it to the server
            % with 'copy' set to true.
            %
            % EXAMPLE
            % o.fileInFileOut('preprocess','inFile',{'f1.mat','f2.mat','f3.mat',f4.mat','f5.mat'},'inPath','/work/data/','outPath','/work/results/','outTag','.preprocessed','mode',1)
            % Will start jobs that calls the preprocess function like this
            % (for each fo the f1..f5)
            % results = preprocess('/work/data/f1.mat','mode',1);
            % (Note how the 'mode' argument is passed to the user
            % defined preprocess function: all parm/value pairs that are
            % not recognized by the fileInFileOut function are passed that
            % way).
            % After completing the job, the results will be saved in
            % /work/results/f1.preprocessed.mat
            %
            % The user is responsible for making sure that the f1.mat etc.
            % exust in 'inPath' on the server, and for retrieving the
            % results from the 'outPath'. (Presumably using something like
            % rsync, otuside Matlab.

            p=inputParser;
            p.addParameter('inFile','',@(x) (ischar(x)|| iscell(x)));
            p.addParameter('inPath','',@ischar);
            p.addParameter('outTag','.processed',@ischar);
            p.addParameter('outPath','',@ischar);
            p.addParameter('batchOptions',{});
            p.addParameter('runOptions','');
            p.addParameter('debug',false);
            p.addParameter('copy',false);
            p.KeepUnmatched = true;
            p.parse(varargin{:});


            %% Prepare the list of files and paths
            if ischar(p.Results.inFile)
                inFile = { p.Results.inFile}; % single file
            else
                inFile = p.Results.inFile(:);
            end

            nrFiles= size(inFile,1);
            if ischar(p.Results.inPath)
                inPath = repmat({p.Results.inPath},[nrFiles 1]); % % One path per in File.
            elseif numel(p.Results.inPath) == nrFiles
                inPath = p.Results.inPath(:);
            else
                error('The number of inPath does not match the number of inFile');
            end

            % Out files are infile+tag
            outFile= cell(size(inFile));
            for i=1:nrFiles
                [~,f,e] = fileparts(inFile{i});
                outFile{i} = [f p.Results.outTag e];
            end
            outPath = p.Results.outPath;


            %% Setup the jobs
            uid = char(datetime("now",'Format','yy.MM.dd_HH.mm.SS.sss'));
            if ~ischar(fun)
                error('The fun argument must be the name of an m-file');
            end
            jobName = [fun '-' uid];
            jobDir = strrep(fullfile(o.remoteStorage,jobName),'\','/');
            localDataFile = fullfile(o.localStorage,[uid '_data.mat']);
            remoteDataFile = strrep(fullfile(jobDir,[uid '_data.mat']),'\','/');



            save(localDataFile,'inFile','outFile','inPath','outPath','-v7.3'); % 7.3 needed to allow partial loading of the data in each worker.
            % Copy the data file to the cluster
            o.put(localDataFile,jobDir);


            if ~isempty(fieldnames(p.Unmatched))
                args = p.Unmatched;
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
            o.sbatch('jobName',jobName,'uniqueID','auto','batchOptions',p.Results.batchOptions,'mfile','slurm.fileInFileOutRun','mfileExtraInput',{'dataFile',remoteDataFile,'argsFile',remoteArgsFile,'mFile',fun,'nodeTempDir',o.nodeTempDir},'debug',p.Results.debug,'runOptions',p.Results.runOptions,'nrInArray',nrFiles,'taskNr',1);


        end

        function jobName = batch(o,fun,varargin)
            % Evaluate the mfile (fun) in each of the 'nrWorkers'
            %
            % The mfile can be a script or a function that takes
            % parameter/value pairs specified in the call to fun as its input.
            %
            % You can also specify fun as a matlab expression containing multiple
            % commands. mlsurm.batch will then execute the expression on
            % the workers on the cluster. To help you keep track, you
            % should specify 'expressionName' to give a helpful name to the
            % expression (and from that, the name of the job on SLURM).appde
            %
            % The following parm/value pairs control SLURM scheduling and
            % are not passed to the function. Some of these have default
            % values set in the cluster object. Anything set here will
            % overrule those defaults.
            %
            % 'batchOptions' - passed to slurm the same way as in
            %                   mslurm.sbatch
            % 'runOptions'  = also passed to slurm. See mslurm.sbatch
            % 'copy' set to true to copy the mfile (fun) to the server.
            % Useful if the mfile you're executing is self-contained and
            % does not yet exist on the server. Note that it will be copied
            % to the .remoteStorage location and added to the search path.
            %
            % 'workingDirectory' Define the working directory.
            % 'addPath' - Add this cell array of paths to the path on the cluster
            % 'nrWorkers'  - The number of workers that should execute this
            %               function.
            % To retrieve the output each of the evaluations of fun, this
            % funciton returns a unique 'tag' that can be passed to
            % mslurm.retrieve().
            %
            p=inputParser;
            p.addRequired('o');
            p.addRequired('fun',@(x) ischar(x) || isstring(x));
            p.addParameter('batchOptions',o.batchOptions,@iscell);
            p.addParameter('runOptions',o.runOptions,@ischar);
            p.addParameter('debug',false,@islogical);
            p.addParameter('copy',false,@islogical);
            p.addParameter('startupDirectory',o.startupDirectory,@ischar);
            p.addParameter('workingDirectory',o.workingDirectory,@ischar);
            p.addParameter('addPath',o.addPath,@(x) ischar(x) ||iscellstr(x) );
            p.addParameter('nrWorkers',1);
            p.addParameter('expressionName','expression');
            p.KeepUnmatched = true;
            p.parse(o,fun,varargin{:});

            if ischar(p.Results.addPath)
                addPth = {p.Results.addPath};
            else
                addPth = p.Results.addPath;
            end
            % Name the job after the current time. Assuming this will be
            % unique.

            if contains(fun,{'(',';',','})
                % This is an expression not an mfile. Create a temporary
                mfilename =fullfile(tempdir,[p.Results.expressionName '.m']);
                fid = fopen(mfilename,'w');
                fprintf(fid,'%s\n',fun);
                fclose(fid);
                fun =p.Results.expressionName;
                copy = true; % Have to copy
            else
                mfilename = which(fun);
                copy = p.Results.copy;
            end
            jobName =[fun '-' o.uid];
            jobDir = strrep(fullfile(o.remoteStorage,jobName),'\','/');
            % Create a (unique) directory on the head node to store data and results.
            result = o.command(['mkdir ' jobDir]); %#ok<NASGU>

            %% Find the unmatched and save as input arg.
            if ~isempty(fieldnames(p.Unmatched))
                args = p.Unmatched;
                argsFile = [jobName '_args.mat'];
                % Save a local copy of the args
                localArgsFile = fullfile(o.localStorage,argsFile);
                remoteArgsFile = strrep(fullfile(jobDir,argsFile),'\','/');
                save(localArgsFile,'args');
                % Copy the args file to the cluster
                o.put(localArgsFile,jobDir);
            else
                remoteArgsFile ='';
            end

            if copy
                % Copy the mfile to remote storage.
                o.put(mfilename,jobDir);
                addToPath = cat(2,addPth,jobDir);
            else
                addToPath = addPth;
            end

            %% Start the jobs
            opts.jobName = jobName;
            opts.uniqueID = 'auto';
            opts.batchOptions = p.Results.batchOptions;
            opts.mfile ='mslurm.batchRun'; % This is the function that will interpret the input args and pass them to fun
            opts.mfileExtraInput ={'argsFile',remoteArgsFile,'mFile',fun,'nodeTempDir',o.nodeTempDir,'jobDir',jobDir};
            opts.debug = p.Results.debug;
            opts.runOptions= p.Results.runOptions;
            opts.nrInArray = p.Results.nrWorkers;
            opts.startupDirectory = p.Results.startupDirectory;
            opts.addPath = addToPath;
            opts.taskNr =1;
            o.sbatch(opts);

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
            % 'data' must be a numeric, cell, or struct array.
            %
            % The following parm/value pairs control SLURM scheduling and
            % are not passed to the function
            % 'batchOptions' - passed to slurm the same way as in
            %                   mslurm.sbatch
            % 'runOptions'  = also passed to slurm. See mslurm.sbatch
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
            % mslurm.retrieve().
            %
            %
            % EXAMPLE
            % data = 10x1 struct array with your data.
            % analyzeMyData = a function that takes one element of the
            % struct array as its input, and returns one output (any matlab
            % variable).
            %
            % tag = o.feval('analyzeMyData',data);
            % will copy the data struct to the server, then start 10 jobs,
            % the first job/worker  will evaluate
            % output1 = analyzeMyData(data(1));
            % the second worker;
            % output2 = analyzeMyData(data(2));
            % etc,
            % Once everything has completed (check mslurmApp you can get
            % the results with
            % results = o.retrieve(tag);
            %  In tis example results{1} will contain output1, results{2}
            %  will contain output2 etc.
            %
            % Here is a specific example where we use a numeric data input
            % data = 1:10;
            % i.e. worker 1 will get '1' as its input, worker 2 will get
            % '2' etc,
            % and the analysis that we do on this data is the function
            % 'rand'.
            % data = 1:10;
            % tag = o.feval('rand',data) %
            % % Wait a while...
            % results  = retrieve(tag);
            % results{1} will contain the result of rand(1);
            % results{2} = rand(2);
            % ...
            % results{10} = rand(10);
            %
            %

            if ~ischar(fun)
                error('The fun argument must be the name of an m-file');
            end
            if ~(iscell(data) || isnumeric(data) || isstruct(data) || ischar(data))
                error('Data must be numeric, cell, or struct');
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
            id = o.uid;
            jobName = [fun '-' id];

            localJobDir = fullfile(o.localStorage,jobName);
            if ~exist(localJobDir,'dir')
                mkdir(localJobDir);
            end
            remoteJobDir = strrep(fullfile(o.remoteStorage,jobName),'\','/');
            % Create a (unique) directory on the head node to store data and results.
            if ~o.exist(remoteJobDir,'dir')
                result = o.command(['mkdir ' remoteJobDir]); %#ok<NASGU>
            end
            % Save a local copy of the input data

            localDataFile = fullfile(localJobDir,'input.mat');
            save(localDataFile,'data','-v7.3'); % 7.3 needed to allow partial loading of the data in each worker.
            % Copy the data file to the cluster
            o.put(localDataFile,remoteJobDir);
            remoteDataFile= strrep(fullfile(remoteJobDir,'input.mat'),'\','/');

            p=inputParser;
            p.addParameter('batchOptions',o.batchOptions);
            p.addParameter('runOptions',o.runOptions);
            p.addParameter('debug',false);
            p.addParameter('copy',false);
            p.addParameter('startupDirectory',o.startupDirectory);
            p.addParameter('addPath',o.addPath,@(x) iscellstr(x) || ischar(x));
            p.KeepUnmatched = true;
            p.parse(varargin{:});
            if ischar(p.Results.addPath)
                addPth = {p.Results.addPath};
            else
                addPth = p.Results.addPath;
            end

            if ~isempty(fieldnames(p.Unmatched))
                args = p.Unmatched;
                argsFile = 'args.mat';
                % Save a local copy of the args
                localArgsFile = fullfile(localJobDir,argsFile);
                remoteArgsFile = strrep(fullfile(remoteJobDir,argsFile),'\','/');
                save(localArgsFile,'args');
                % Copy the args file to the cluster
                o.put(localArgsFile,remoteJobDir);
            else
                remoteArgsFile ='';
            end

            if p.Results.copy
                % Copy the mfile to remote storage.
                mfilename = which(fun);
                o.put(mfilename,remoteJobDir);
            end

            %% Start the jobs
            opts.jobName = jobName;
            opts.batchOptions = p.Results.batchOptions;
            opts.mfile ='mslurm.fevalRun';
            opts.mfileExtraInput ={'dataFile',remoteDataFile,'argsFile',remoteArgsFile,'mFile',fun,'nodeTempDir',o.nodeTempDir,'jobDir',remoteJobDir};
            opts.debug = p.Results.debug;
            opts.runOptions= p.Results.runOptions;
            opts.nrInArray = nrDataJobs;
            opts.startupDirectory = p.Results.startupDirectory;
            opts.addPath = addPth;
            opts.taskNr =1;
            o.sbatch(opts);

        end


        function jobInfo = taskBatch(o,fun,data,args,varargin)
            %function jobInfo = taskBatch(o,fun,data,args,varargin)
            %
            %Users can either upload a dataset via this function to process
            %their data via their function 'fun' or they can provide a jobID
            %from a previous job or a jobName of a previous job to re-use data
            %that was uploaded with that previous job (or attempt).
            %
            %In case the user uploads their data, then a call to taskBatch will look
            %approximately like this:
            %       jobInfo = taskBatch('userFun',data,args,'uniqueOutputName',false,'collateFun','userCollateFun','outputFolder','me_12Mar2018/frequencyAnalysis/','addJobName','merry_frequencyAnalysis','batchOptions',{'time','23:50:00','partition','day-long'});
            %       userCollateFun can either be a function handle such as userCollateFun = @(x) userFun(x,'action','collate'), to pass additional input to the user's function.
            %       If no additional input arguments are necessary then collate fun can also just be the name of the function (in quotation marks) to be used for collating results ('collateFunName').
            %       If collateFun is empty then the result will be a struct array in the form of result(1:nrTasks).result...
            %
            %In case the user wants to re-use data that was already uploaded to
            %the cluster with a previous job, the only difference will be what
            %is specified in 'data'.
            %       It will either be a jobID ->
            %           jobInfo = taskBatch('userFun','110055',args,...)
            %       Or a jobName
            %           jobInfo =
            %           taskBatch('userFun','testCoherence_me_12Mar2018_frequencyAnalysis_180206_025611',args,...)
            %
            % If the result of the call to taskBatch should stay unique, the user can set 'uniqueOutputName' to true so that a subfolder in
            % the form of \yymmdd_hhmmss\ will be generated and the result will be transferred to that folder.
            %
            % AS Feb-2018


            %did the user provide additional inut arguments for the function they want
            %to run on the cluster
            if isempty(args)
                error(['No input arguments provided for the function "' fun '" to run on the cluster. Provide at least "args.tasks" or consider using o.feval or o.sbatch instead of o.taskBatch.'])
            end

            %how many jobs should be submitted depends on the size of args.tasks
            if iscell(data)
                if isempty(args)
                    error('No input provided for "args". Either specify that variable or consider using either o.feval or o.sbatch instead of taskBatch to submit jobs.');
                end
                dataType = 'realData';
            elseif ischar(args)
                error('"args" cannot be strings but needs to be real data');
            elseif ischar(data)
                %this option is reserved for referencing previously submitted jobs
                %to re-use data that already exists on the cluster
                if ~isequal(sum(isletter(data)),0)
                    mslurm.log(['checking whether data from the job with the name: "' data '" still exists...'])
                    dataType = 'jobName';
                elseif isequal(sum(isletter(data)),0)
                    mslurm.log(['checking whether data from the job with the ID: "' data '" still exists...'])
                    dataType = 'jobID';
                end

            else  %we really shouldn't end up here...
                error('There is something wrong with either "data" or "args". Please look into what you submitted. "data" needs to be a struct (-array) or cell array and "args" needs to be a struct or struct array.');
            end

            %how many tasks should be submitted
            if isequal(length(args),1)
                try
                    if iscell(args.tasks)
                        nrInArray = length(args.tasks);
                    end
                catch
                    error('"args" must be a struct that contains the field "tasks" which should be a cell array which size defines the number of tasks submitted.');
                end
            end
            %put data and args into the right format
            if isrow(data) && ~ischar(data)
                % This is interpreted as a column.
                data = data';
            end
            if isrow(args)
                args = args';
            end


            %InputParser/Parameters:
            p = inputParser;
            p.StructExpand = true;
            p.KeepUnmatched = true;
            addParameter(p,'batchOptions',{});                              %instructions that include information such as wall-time, and partition type
            addParameter(p,'runOptions','');
            addParameter(p,'uniqueOutputName',false);                        %will the final collated filename reflect the uniqueID that is automatically generated?
            %true [default]: the same analysis can be performed multiple times and none will be overwritten
            %false: the next time that this analysis is run, previous data will be overwritten
            addParameter(p,'collateFun','');                                %allow user to specify a function that knows how to collate their data
            addParameter(p,'outputFolder',[],@ischar);                      %an outputFolder(with subfolders) to allow the user to keep their analysis organized and distinguishable in their folder
            addParameter(p,'addJobName',[],@ischar);                        %additional info that will be part of the jobName on the cluster
            addParameter(p,'from',7);                                       %if data is a jobID or job(Group)Name, then specify how many days ago to original dataset was submitted
            %default: 7 days ago;
            addParameter(p,'startupDirectory','',@ischar);% Matlab will startup in this directory (-sd command line argument)
            addParameter(p,'debug',false);

            %if a task of a whole taskBatch fails, then we don't want to resubmit the whole Batch
            %this parameter should not be used directly, it should only be used by o.retry
            addParameter(p,'resubmitTaskNr',false);
            parse(p,varargin{:});

            %if tasks has not been provided then this function cannot run
            if isempty(args(1).tasks)
                error('The input arguments do not have a field "tasks". Use that field to provide instructions for what each node/worker is supposed to do')
            end

            %check if 'data' is real data or if it just refers to an already existing dataset
            switch dataType
                case 'jobID' %if we only know the jobID, then we need to find the jobName to find out what folder the data got uploaded to for that previous job
                    tempCluster = mslurm; % Get the default mslurm
                    tempCluster.sacct('starttime',datetime("now")-p.Results.from); %Update sacct
                    allJobIDs = {tempCluster.jobs.JobID};
                    targetJobIDIdx = find(contains(allJobIDs,data));
                    if isempty(targetJobIDIdx)
                        error(['No job with the ID ' data ' could be found within the last ' num2str(p.Results.from) ' days'])
                    end
                    allJobNames = {tempCluster.jobs.JobName};
                    targetJobName = allJobNames{targetJobIDIdx(1)};
                    targetJobFolder = strrep(fullfile(o.remoteStorage,erase(targetJobName,'-taskBatch'),'/'),'\','/');
                    remoteDataFile = [targetJobFolder erase(targetJobName,'-taskBatch') '_data.mat'];
                    localDataFile = [o.localStorage erase(targetJobName,'-taskBatch') '_data.mat']; %#ok<NASGU>
                case 'jobName' %if we know the jobName then we basically know the folder already
                    targetJobFolder = strrep(fullfile(o.remoteStorage,erase(data,'-taskBatch'),'/'),'\','/');
                    targetJobName = data;
                    remoteDataFile = [targetJobFolder erase(data,'-taskBatch') '_data.mat'];
                    localDataFile = [o.localStorage erase(data,'-taskBatch') '_data.mat']; %#ok<NASGU>
                case 'realData'
                    remoteDataFile = [];
            end

            %deal with different ways to specify a collateFunction
            if ~isempty(p.Results.collateFun)
                collateFun = getByteStreamFromArray(char(p.Results.collateFun));
                collateFun = num2str(collateFun);
            else
                collateFun = [];
            end

            %check if folder and data still exist
            if ~isempty(remoteDataFile)
                if o.exist(targetJobFolder,'dir')
                    if ~o.exist([targetJobFolder erase(targetJobName,'-taskBatch') '_data.mat'])
                        error(['the data file "' erase(targetJobName,'-taskBatch') '_data.mat"  does not exist anymore. You need to upload data for your job']);
                    else
                        mslurm.log('... data found')
                    end
                else
                    error(['the folder "' targetJobFolder '"  does not exist anymore. You need to upload data for your job']);
                end
            end

            %create the jobGroup that is send to the cluster
            %create a unique jobName for the group of tasks that is run on the cluster
            %but users are allowed to name the final output file, which
            %will be reflected in the name of the jobGroup
            id = char(datetime("now",'Format','yyMMdd_HHmmSS'));

            %where should the collated result from the taskBatch be saved
            if ~isempty(p.Results.outputFolder) %the user knows exactly where the result should end up
                if p.Results.uniqueOutputName
                    finalFolderName =  [getenv('USERNAME') '/' p.Results.outputFolder id '/'];
                else
                    finalFolderName =  [ getenv('USERNAME') '/' p.Results.outputFolder];
                end
            else
                if ~isempty(p.Results.addJobName) %the user doesn't have a specific folder structure in mind
                    %but a clear idea how the job should be called and we
                    %can use that to create that final folder
                    if p.Results.uniqueOutputName
                        finalFolderName =  [getenv('USERNAME') '/' p.Results.addJobName '_' id '/'];
                    else
                        finalFolderName =  [getenv('USERNAME') '/' p.Results.addJobName '/'];
                    end
                else %the user has now opinion on how to call the job and where to save the result
                    %this might be problematic if the same function will run on future jobs
                    %because those separete results will be hard to distinguish
                    finalFolderName =  [getenv('USERNAME') '/' fun '_' id '/'];
                    mslurm.log('consider specifying either "jobName" or "outputFolder" or both so make your job and/or the folder where the results be saved more distinguishable. In particular if you are going to use this function for future but different jobs')
                end
            end

            %how should the job be called on the cluster
            if ~isempty(p.Results.addJobName)
                jobGroupName = cat(2,fun,'_', p.Results.addJobName, '_', id);
            else
                jobGroupName = cat(2,fun,'_', id);
            end

            %create directories on the cluster where temporary and final results can be stored
            %1. a folder in the headRootDir folder where the collated
            %result will be saved for good
            %1.1 check if the users's folder exists and create
            %that folder if it doesnt
            userFolderDir = strrep(fullfile(o.headRootDir, getenv('USERNAME')),'\','/');
            if ~o.exist(userFolderDir,'dir')
                result = o.command(['mkdir ' userFolderDir]);
            end
            %1.2 create the folder for the collated result within the user's folder
            finalFolderDir = strrep(fullfile(o.headRootDir,finalFolderName, '/'),'\','/');
            %since we cannot add folders with many subfolders
            %in one go, we need to split this into a loop
            tempFinalFolder =  erase(finalFolderDir,userFolderDir);
            appendedFolderName = userFolderDir;
            folderIdx = strfind(tempFinalFolder,'/');
            startIdx = 1;
            subFolderNames = cell(1,numel(folderIdx)-1);
            for folderCntr = 1:numel(folderIdx)
                subFolderNames{folderCntr} = tempFinalFolder(startIdx:folderIdx(folderCntr));
                startIdx = folderIdx(folderCntr)+1;
                appendedFolderName = [appendedFolderName subFolderNames{folderCntr}]; %#ok
                if ~o.exist(appendedFolderName,'dir')
                    result = o.command(['mkdir ' appendedFolderName]);
                end
            end


            %2. create a folder in the remoteStorage to save the task results from each worker,
            jobGroupDir = strrep(fullfile(o.remoteStorage,jobGroupName),'\','/');
            if ~o.exist(jobGroupDir,'dir')
                result = o.command(['mkdir ' jobGroupDir]);
            end


            %create a file with input arguments that get passed to the function
            %that will run on the cluster
            argsFile = [jobGroupName '_args.mat'];
            % Save a local copy of the args
            localArgsFile = fullfile(o.localStorage,argsFile);
            remoteArgsFile = strrep(fullfile(o.remoteStorage,jobGroupName,argsFile),'\','/');
            save(localArgsFile,'args','-v7.3'); % 7.3 needed to allow partial loading of the args in each worker.
            % Copy the args file to the cluster
            if ~p.Results.debug
                o.put(localArgsFile,jobGroupDir);
            end

            %specify the file(s) that the function should run on and upload
            %it to the cluster (if necessary)
            if isempty(remoteDataFile)
                % Save a local copy of the input data
                localDataFile = fullfile(o.localStorage,[jobGroupName '_data.mat']);
                remoteDataFile = strrep(fullfile(o.remoteStorage,jobGroupName,[jobGroupName '_data.mat']),'\','/');
                save(localDataFile,'data','-v7.3'); % 7.3 needed to allow partial loading of the data in each worker.
                % Copy the data file to the cluster
                if ~p.Results.debug
                    o.put(localDataFile,jobGroupDir);
                end
            end

            %run all tasks as an arrayJob
            jobID = o.sbatch('jobName',[jobGroupName '-taskBatch'] ,'uniqueID','','batchOptions',p.Results.batchOptions,'mfile','mslurm.taskBatchRun','mfileExtraInput',{'dataFile',remoteDataFile,'argsFile',remoteArgsFile,'mFile',fun,'nodeTempDir',o.nodeTempDir,'jobDir',jobGroupDir,'userDir',userFolderDir},'runOptions',p.Results.runOptions,'nrInArray',nrInArray,'debug',p.Results.debug,'startupDirectory',p.Results.startupDirectory);


            %start a collate job
            if ~isempty(jobID)  %jobs have been submitted succesfully
                dependency = sprintf('afterany:%s',num2str(jobID));  %execute this after the arrayJob has been succesfully submitted

                %run the collateJob (via sbatch, which will run taskBatchRun)
                collateJobId  = o.sbatch('jobName',[jobGroupName '-collate'],'uniqueID','','batchOptions',cat(2,p.Results.batchOptions,{'dependency',dependency}),'mfile','mslurm.taskBatchRun','mfileExtraInput',{'argsFile',remoteArgsFile,'mFile',collateFun,'nodeTempDir',o.nodeTempDir,'jobDir',jobGroupDir,'userDir',finalFolderDir,'totalNrTasks',nrInArray},'runOptions',p.Results.runOptions,'nrInArray',1,'taskNr',0,'debug',p.Results.debug,'startupDirectory',p.Results.startupDirectory);
            end

            jobInfo.taskIds = jobID;
            jobInfo.collateJobId = collateJobId;
            jobInfo.result = result;

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
            % without further processing. This is used by the mslurm.retry
            % function.
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
            % See also mslurm/feval or mslurm.batch for example functions that use
            % mslurm/sbatch to submit jobs to the scheduler.

            p = inputParser;
            p.addParameter('jobName',o.uid,@ischar );
            p.addParameter('batchOptions',o.batchOptions,@iscell);
            p.addParameter('runOptions','',@ischar);
            p.addParameter('mfile','',@ischar);
            p.addParameter('mfileExtraInput',{},@iscell); % pv pairs
            p.addParameter('command','',@ischar);
            p.addParameter('nrInArray',0,@(x) (isnumeric(x) && isscalar(x)));
            p.addParameter('debug',false,@islogical);
            p.addParameter('retryBatchFile','',@ischar);
            p.addParameter('taskNr',0,@isnumeric);
            p.addParameter('startupDirectory',o.startupDirectory,@ischar);% Matlab will startup in this directory (-sd command line argument)
            p.addParameter('workingDirectory',o.workingDirectory,@ischar);
            p.addParameter('addPath',o.addPath,@(x) ischar(x) || iscellstr(x)); % Add these folders to the Matlab path by calling addpath(x)
            p.parse(varargin{:});
            if ischar(p.Results.addPath)
                addPth = {p.Results.addPath};
            else
                addPth = p.Results.addPath;
            end
            if isempty(p.Results.retryBatchFile)
                %Construct a new batch file for sbatch
                jobName = p.Results.jobName;
                remoteJobDir = strrep(fullfile(o.remoteStorage,jobName),'\','/');
                localJobDir = fullfile(o.localStorage,jobName);
            
            
                if ~isempty(p.Results.mfile)
                    % Wrap an mfile
                    extraIn = '';
                    for i=1:2:length(p.Results.mfileExtraInput)
                        if ischar(p.Results.mfileExtraInput{i+1}) || isstring(p.Results.mfileExtraInput{i+1})
                            strValue = ['''' p.Results.mfileExtraInput{i+1} ''''];
                        elseif isnumeric(p.Results.mfileExtraInput{i+1})
                            strValue = ['[' num2str(p.Results.mfileExtraInput{i+1}) ']'];
                        else
                            error('Extra input to the mfile must be parm/value pairs. Value can only be numeric or string');
                        end
                        extraIn = cat(2,extraIn,',''', p.Results.mfileExtraInput{i},''',',strValue);
                    end

                    if ~isempty(p.Results.startupDirectory)
                        sd = ['-sd '  p.Results.startupDirectory];
                    else
                        sd = '';
                    end
                    addPth =strjoin(cat(2,addPth,o.mslurmFolder),':');
                    if ~isempty(addPth)
                        addPathStr = sprintf('addpath(''%s'')',addPth);
                    else
                        addPathStr = '';
                    end

                    if isempty(p.Results.workingDirectory)
                        % Run in remoteStorage directoryc
                        wd = remoteJobDir;
                    else
                        % Run in specified working Directory
                        wd = p.Results.workingDirectory;
                    end
                    if p.Results.nrInArray>=1
                        runStr = ['%s/matlab  ' sd ' -nodisplay -nodesktop -r  "try;%s;cd ''%s'';%s($SLURM_JOB_ID,$SLURM_ARRAY_TASK_ID %s);catch me;mslurm.exit(me);end;mslurm.exit(0);"'];
                        run = sprintf(runStr,o.matlabRoot,addPathStr,wd,p.Results.mfile,extraIn);
                    else
                        runStr = ['%s/matlab ' sd ' -nodisplay -nodesktop -r "try;%s;cd ''%s''; %s($SLURM_JOB_ID,%d %s);catch me;mslurm.exit(me);end;mslurm.exit(0);"'];
                        run = sprintf(runStr,o.matlabRoot,addPathStr,wd,p.Results.mfile,p.Results.taskNr,extraIn);
                    end
                elseif ~isempty(p.Results.command)
                    % The user knows what to do. Run this command as is with srun.
                    run = p.Results.command;
                else
                    error('command and mfile cannot both be empty...');
                end


                  if p.Results.nrInArray>=1
                    % Ensure that array job numbers generated by slurm are
                    % base-1 (default is base zero)
                    batchOpts = cat(2,p.Results.batchOptions,{'array',['1-' num2str(p.Results.nrInArray)]});
                    outFile = [mslurm.STDOUTFILE '_%a.out'];
                else
                    batchOpts = p.Results.batchOptions;
                    outFile = mslurm.STDOUTFILE;
                end
                % Options with empty values are removed.
                empty = find(cellfun(@isempty,batchOpts(2:2:end)));
                batchOpts([2*empty-1 2*empty])= [];

                batchFile = mslurm.SBATCHFILE;                
                fid  =fopen(fullfile(localJobDir,mslurm.SBATCHFILE) ,'w'); % no t (unix line endings are needed)
                fprintf(fid,'#!/bin/bash\n');
                batchOpts = cat(2,batchOpts, {'job-name',jobName,'output',outFile,'error',outFile});
                for opt=1:2:numel(batchOpts)
                    if isempty(batchOpts{opt+1})
                        fprintf(fid,'#SBATCH --%s\n',batchOpts{opt});
                    elseif isnumeric(batchOpts{opt+1})
                        fprintf(fid,'#SBATCH --%s=%d\n',batchOpts{opt},batchOpts{opt+1});
                    elseif ischar(batchOpts{opt+1})
                        isSpace  = batchOpts{opt+1}==' ';
                        if any(isSpace)
                            batchOpts{opt+1}(isSpace) = '';
                            mslurm.log(['Removing spaces from Slurm Batch Option ''' batchOpts{opt} ''' now set to:''' batchOpts{opt+1} '''']);
                        end
                        fprintf(fid,'#SBATCH --%s=%s\n',batchOpts{opt},batchOpts{opt+1});
                    else
                        error('sbatch options should be char or numeric');
                    end
                end
                fprintf(fid,'srun %s %s\n',p.Results.runOptions,run);
                fclose(fid);
            else

                %TODO extract job file 
                % Use the specified batch file. Extract the job name.\
                batchFile = p.Results.retryBatchFile;
                % fid = fopen(fullfile(o.localStorage,batchFile));
                % while (fid >0 && = fgetl(fid) )
                % end
                jobName   = strsplit(batchFile,'.');
                jobName = jobName{1};
            end

            if p.Results.debug
                mslurm.log ('Debug mode. Nothing will be submitted to SLURM')
                edit (fullfile(localJobDir,batchFile));
                jobId=0;result = 'debug mode';
            else
                if isempty(p.Results.retryBatchFile)
                    % Copy the flie to the cluster
                    o.put(fullfile(localJobDir,batchFile),remoteJobDir);
                end
                % Start the sbatch
                [result,err] = o.command(sprintf('cd %s ;sbatch %s/%s',remoteJobDir,remoteJobDir,batchFile));
                jobId = str2double(regexp(result{1},'\d+','match'));
                if iscell(err)
                    err = err{1};
                end
                if isempty(jobId) || isnan(jobId)
                    mslurm.log(['Failed to submit ' jobName ' (Msg=' result{1} ', Err: ' err{1} ' )']);
                else
                    mslurm.log(['Successfully submitted ' jobName ' (JobID=' num2str(jobId) ')']);
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
            %mslurm.log(result{1})
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
            p.addParameter('maxNrFails',10,@isnumeric);
            p.addParameter('selection',[],@isnumeric); %
            p.addParameter('jobId',[],@(x)(ischar(x) || iscell(x)));
            p.parse(varargin{:});

            if o.nrJobs==0
                list = {};
                mslurm.log('No jobs found')
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
                jobIDs = {o.jobs(jobIx).JobID};

                % Four kinds of jobs, with different retry needs
                isArray = cellfun(@(x) (any(x=='_')),jobIDs);
                isTaskBatch =    isArray & contains({o.jobs(jobIx).JobName},'taskBatch');
                isRegularArray = isArray & ~isTaskBatch;
                isRegularJob =~(isTaskBatch | isRegularArray);
                % TODO: this depends on the ordering of the jobs.... Seems a risky strategy?
                if jobIx>1
                    isTaskBatchCollate = (contains({o.jobs(jobIx-1).JobName},'taskBatch') & contains({o.jobs(jobIx).JobName},'-collate')) | contains({o.jobs(jobIx).JobName},'-tbRCollate');
                else
                    isTaskBatchCollate =false;
                end
                %
                if isTaskBatchCollate
                    isTaskBatch = 1;
                    isRegularArray = 0;
                    isRegularJob = 0;
                end


                %% Retry regular jobs
                if any(isRegularJob)
                    jobIx = jobIx(isRegularJob);  % Regular job
                    list = {o.jobs(jobIx).JobName};

                    alreadyRetried = {};
                    for j=jobIx
                        batchFile = mslurm.decodeComment(o.jobs(j).Comment,'sbatch');
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
                %% Retry taskBatchJobs and taskBatch collateJobs
                if any(isTaskBatch)
                    taskBatchJobIx = jobIx(isTaskBatch);
                    list = {o.jobs(taskBatchJobIx).JobName};

                    %only one taskBatch should be retried at a time
                    uTaskBatch = unique(list);
                    if length(uTaskBatch)>1
                        mslurm.log('Sorry, only retry one taskBatch at a time. Please retry separately. \n');
                    else

                        if isTaskBatchCollate
                            resubmitAnswer = questdlg('Resubmit AS IS, or change Imputs?', ...
                                'Resubmission Options', ...
                                'Resubmit','Change Inputs','Cancel','Cancel');

                            if strcmpi(resubmitAnswer,'Change Inputs')
                                elementInArray = 1;
                                locFullFile = getFile(o,jobIDs{1},'type','sh');
                                [pth,locFile] = fileparts(locFullFile);
                                newFile = strsplit(locFile,'-');
                                newBatchFile = [newFile{1} '-tbRCollate.sh'];
                                srcFid =fopen(locFullFile,'r');
                                trgFid =fopen(fullfile(pth,newBatchFile),'w');
                                while (true)
                                    line = fgetl(srcFid);
                                    if line==-1;break;end
                                    if contains(line,'#SBATCH --array')
                                        continue;%skip
                                    elseif contains(line,'#SBATCH --output') || contains(line,'#SBATCH --error')
                                        line = strrep(line,'%A_%a.out','%A.out');
                                    elseif contains(line,'#SBATCH --job-name')
                                        tmp = strsplit(line,'=');
                                        tmp = strsplit(tmp{2},'-');
                                        line = ['#SBATCH --job-name=' tmp{1} '-tbRCollate'];
                                    elseif contains(line,'#SBATCH --comment')
                                        line = ['#SBATCH --comment=sbatch:' newBatchFile];
                                    elseif contains(line,'srun  matlab')
                                        line = strrep(line,'$SLURM_ARRAY_TASK_ID',num2str(elementInArray));
                                    elseif contains(line,'#SBATCH --time')
                                        startInputIdx = strfind(line,'time=')+5;
                                        %  if strcmpi(resubmitAnswer,'Change Inputs')
                                        jobParmPrompt = {'wall-time:'};
                                        dlgtitle = 'Change Wall-Time?';
                                        dims = [1 35];
                                        definput = {line(startInputIdx:end)};
                                        jobParmAnswer = inputdlg(jobParmPrompt,dlgtitle,dims,definput);
                                        line = ['#SBATCH --time=' jobParmAnswer{1}];
                                        % else
                                        %  line = ['#SBATCH --time' line(startInputIdx:end)];
                                        % end

                                    elseif contains(line,'#SBATCH --mem')
                                        startInputIdx = strfind(line,'mem=')+4;
                                        % if strcmpi(resubmitAnswer,'Change Inputs')
                                        jobParmPrompt = {'memory (GB)'};
                                        dlgtitle = 'Change Memory?';
                                        dims = [1 35];
                                        definput = {line(startInputIdx:end)};
                                        jobParmAnswer = inputdlg(jobParmPrompt,dlgtitle,dims,definput);
                                        line = ['#SBATCH --mem=' jobParmAnswer{1}];
                                        % else
                                        % line = '#SBATCH --time=12:0:00';
                                        % end

                                    elseif contains(line,'#SBATCH --partition')
                                        %startInputIdx = strfind(line,'partition=')+10;
                                        %if strcmpi(resubmitAnswer,'Change Inputs')
                                        resubmitPartitionAnswer = questdlg('Which Partition to Resubmit to?', ...
                                            'Partition Options', ...
                                            'main','nm3','main,nm3','main,nm3');
                                        line = ['#SBATCH --partition=' resubmitPartitionAnswer];
                                        %  else
                                        %    line = ['#SBATCH --partition=' line(startInputIdx:end)];
                                        % end
                                    end
                                    fprintf(trgFid,'%s\n',line);
                                end
                                fclose(trgFid);
                                fclose(srcFid);

                                o.put(fullfile(o.localStorage,newBatchFile),o.remoteStorage);
                                o.sbatch('retryBatchFile',newBatchFile);

                            elseif strcmpi(resubmitAnswer,'Resubmit')

                                batchFile = mslurm.decodeComment(o.jobs(jobIx).Comment,'sbatch');
                                if length(batchFile)~=1 || isempty(batchFile{1})
                                    error('The comment field should contain the batch file...');
                                else
                                    batchFile = batchFile{1};
                                end

                                o.sbatch('retryBatchFile',batchFile);


                            end
                        else

                            resubmitAnswer = questdlg('What should be resubmitted?', ...
                                'Resubmission Options', ...
                                'Complete TaskBatch','Selected Task Nr(s)','Cancel','Cancel');

                            % Handle response
                            switch resubmitAnswer
                                case 'Complete TaskBatch'
                                    batchFile = mslurm.decodeComment(o.jobs(taskBatchJobIx(1)).Comment,'sbatch');
                                    batchFile = batchFile{1};
                                    o.sbatch('retryBatchFile',batchFile);

                                case 'Selected Task Nr(s)'
                                    %separate jobID from taskID
                                    taskIds = {o.jobs(taskBatchJobIx).JobID};
                                    taskBatchId = taskIds{1}(1:strfind(taskIds{1},'_')-1);
                                    taskIds = strrep(taskIds,[taskBatchId '_'],'');
                                    taskIds = cellfun(@str2num,taskIds);

                                    resubJobName = strrep(uTaskBatch{1},'-taskBatch','');

                                    jobGroupDir = [o.remoteStorage '/' resubJobName];
                                    %remoteDataPath =  [jobGroupDir '/' resubJobName];
                                    fun = resubJobName(1:strfind(resubJobName,'_')-1);
                                    userFolderDir = strrep(fullfile(o.headRootDir,[getenv('USERNAME') '/']),'\','/');


                                    %recreate the input arguments to sbatch form the original shell file
                                    originalInputArgsFile = fileread([o.localStorage '\' resubJobName '-taskBatch.sh']);
                                    originalInputArgsByLine = splitlines(originalInputArgsFile);

                                    %the arguments we want are
                                    extractArgs = {'--time'; '--mem'; ' -sd '; 'dataFile'; 'argsFile'};

                                    %this assumes that the order of the dataFile and argsFile input arguments will not change in the future
                                    for extractArgCntr = 1:length(extractArgs)
                                        whichLine = find(contains(originalInputArgsByLine,extractArgs{extractArgCntr}));
                                        switch extractArgs{extractArgCntr}

                                            case '--time'
                                                wallTime = originalInputArgsByLine{whichLine}(strfind(originalInputArgsByLine{whichLine},'=')+1:end);

                                            case '--mem'
                                                memorySize = originalInputArgsByLine{whichLine}(strfind(originalInputArgsByLine{whichLine},'=')+1:end);

                                            case ' -sd '
                                                firstIdx = strfind(originalInputArgsByLine{whichLine},extractArgs{extractArgCntr})+5;
                                                subString = originalInputArgsByLine{whichLine}(firstIdx:end);
                                                lastIdx = strfind(subString,'-nodisplay')-2;
                                                startupDir = subString(1:lastIdx);

                                            case 'dataFile'
                                                firstIdx = strfind(originalInputArgsByLine{whichLine},extractArgs{extractArgCntr})+11;
                                                subString = originalInputArgsByLine{whichLine}(firstIdx:end);
                                                lastIdx =  strfind(subString,'argsFile')-4;
                                                dataFile = subString(1:lastIdx);

                                            case 'argsFile'
                                                firstIdx = strfind(originalInputArgsByLine{whichLine},extractArgs{extractArgCntr})+11;
                                                subString = originalInputArgsByLine{whichLine}(firstIdx:end);
                                                lastIdx =  strfind(subString,'mFile')-4;
                                                argsFile = subString(1:lastIdx);

                                        end
                                    end

                                    %ask if any value should be changed
                                    jobParmPrompt = {'memory (GB):','wall-time:'};
                                    dlgtitle = 'Need to change Job Parameters?';
                                    dims = [1 50];
                                    definput = {memorySize,wallTime};
                                    jobParmAnswer = inputdlg(jobParmPrompt,dlgtitle,dims,definput);
                                    memorySize = jobParmAnswer{1};
                                    wallTime = jobParmAnswer{2};

                                    %submit every selected task
                                    for taskCntr = 1:numel(taskIds)
                                        o.sbatch('jobName',[resubJobName '-reBatch_task_' num2str(taskIds(taskCntr))],'uniqueID','','batchOptions',{'time',wallTime,'partition','nm3,main','mem',memorySize},'mfile','mslurm.taskBatchRun','mfileExtraInput',{'dataFile',dataFile,'argsFile',argsFile,'mFile',fun,'nodeTempDir',o.nodeTempDir,'jobDir',jobGroupDir,'userDir',userFolderDir},'runOptions','','nrInArray',0,'taskNr',taskIds(taskCntr),'debug',false,'startupDirectory',startupDir);
                                    end

                                case 'Cancel'
                                    mslurm.log('Nothing to submit then. \n');
                                otherwise
                                    mslurm.log('Nothing to submit then. \n');

                            end
                        end
                    end
                end

                %% Retry regular array jobs  (as regular non-array jobs)
                if any(isRegularArray)
                    arrayJobIx = jobIx(isRegularArray);
                    list = {o.jobs(arrayJobIx).JobName};
                    for j=arrayJobIx
                        thisJobID = o.jobs(j).JobID;
                        tmp = strsplit(thisJobID,'_');
                        %arrayJobID = tmp{1};
                        elementInArray = tmp{2};
                        batchFile = mslurm.decodeComment(o.jobs(j).Comment,'sbatch');
                        if length(batchFile)~=1 || isempty(batchFile{1})
                            error('The comment field should contain the batch file...');
                        else
                            batchFile = batchFile{1}; %#ok<NASGU>
                        end
                        % Make a copy
                        locFullFile = getFile(o,thisJobID,'type','sh');
                        [pth,locFile] = fileparts(locFullFile);
                        newFile = strsplit(locFile,'-');
                        newBatchFile = [newFile{1} '-R' elementInArray '.sh'];
                        srcFid =fopen(locFullFile,'r');
                        trgFid =fopen(fullfile(pth,newBatchFile),'w');
                        while (true)
                            line = fgetl(srcFid);
                            if line==-1;break;end
                            if contains(line,'#SBATCH --array')
                                continue;%skip
                            elseif contains(line,'#SBATCH --output') || contains(line,'#SBATCH --error')
                                line = strrep(line,'%A_%a.out','%A.out');
                            elseif contains(line,'#SBATCH --job-name')
                                tmp = strsplit(line,'=');
                                tmp = strsplit(tmp{2},'-');
                                line = ['#SBATCH --job-name=' tmp{1} '-R' elementInArray];
                            elseif contains(line,'#SBATCH --comment')
                                line = ['#SBATCH --comment=sbatch:' newBatchFile];
                            elseif contains(line,'srun  matlab')
                                line = strrep(line,'$SLURM_ARRAY_TASK_ID',elementInArray);
                            elseif contains(line,'#SBATCH --time')
                                line = '#SBATCH --time=12:0:00';
                            end
                            fprintf(trgFid,'%s\n',line);
                        end
                        fclose(trgFid);
                        fclose(srcFid);

                        o.put(fullfile(o.localStorage,newBatchFile),o.remoteStorage);

                        %if ~ismember(batchFile,alreadyRetried)
                        o.sbatch('retryBatchFile',newBatchFile);
                        %     alreadyRetried = cat(2,alreadyRetried,batchFile);
                        % end
                    end
                end
            end
        end


        function [groups,groupIx,subs,jobIx]=jobGroups(o,expression)
            % Often jobs belong together in a group. By using convention
            % jobName =  Job-SubJob, the 'group' (job) and its elements (subjob)
            % can be determined from the jobName. This is used by slurmApp to make the
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
                [match{noGroupMatch}]= deal(struct('group','Jobs not submitted by mslurm','sub',''));
                match = cat(1,match{:});
                subs = {match.sub};
                subs(noGroupMatch)= jobNames(noGroupMatch);
                [jobNames{noGroupMatch}] = deal('Jobs not submitted by mslurm');
                [groups,~,groupIx] = unique({match.group});
                nrGroups = length(groups);
                jobIx = cell(1,nrGroups);
                for i=1:nrGroups
                    jobIx{i} = find(strncmp(groups{i},jobNames,length(groups{i})));
                end
            end
        end


        function [T]=jobsTable(o,expression)
            % Often jobs belong together in a group. By using convention
            % jobName =  Job-SubJob, the 'group' (job) and its elements (subjob)
            % can be determined from the jobName. This is used by mslurmApp to make the
            % interaction with the logs less cluttered.
            if nargin<2
                % Default format  GROUP-SUB
                expression = '(?<group>[\w_-]*)-(?<sub>[\w\d\.]*$)';
            end
            if o.nrJobs >0
                jobNames = {o.jobs.JobName};
                match = regexp(jobNames,expression,'names');
                noGroupMatch = cellfun(@isempty,match);
                [match{noGroupMatch}]= deal(struct('group','Jobs not submitted by mslurm','sub',''));
                match = cat(1,match{:});
                T=struct2table(o.jobs,'AsArray',true);
                T= addvars(T,{match.group}',o.failStateName', 'NewVariableNames',{'Group','FailState'});
            else
                T = table; % Empty table
            end
        end

        function [localFile,msg] = getFile(o,jobId,varargin )
            % Retrieve logging files from the cluster. This can be the redirected
            % stdout (type =out) or stderr (type=err), or the bash shell script (type =sh)
            % Currently sbatch writes both out and err to the same .out file so the
            % 'err' file has no information.
            % INPUT
            % jobId  = jobID
            % 'element' =  element of a job array (ignored for non-array batch
            % jobs).
            % 'type' = 'out' or 'err' or 'sh'
            % 'forceRemote' = Force reading from remote [true].
            % OUTPUT
            %  localFile = The full name of the file on the local/client
            %  computer. This can be a cell array if multiple files are
            %  requested (ie. for multiple jobids)
            % msg = Error messages.
            %  If the use provides no output argument, the file is opened in the editor
            %
            p =inputParser;
            p.addParameter('element',[],@isnumeric);% Element of array job
            p.addParameter('forceRemote',true,@islogical);% force reading from remote storage
            p.addParameter('type','out',@(x) (ischar(x) && ismember(x,{'out','err','sh'})));
            p.parse(varargin{:});

            %% Construct the file name
            % For running jobs we use scontrol for completed jobs sacct.
            % (scontrol only works for running jobs, sacct does not have
            % the comment while the task is running. Strange but true.
            if ischar(jobId);jobId= {jobId};end
            nrFiles = numel(jobId);
            localFile= cell(1,nrFiles);
            msg = cell(1,nrFiles);
            [msg{:}] = deal('');
            [tf,ix] = ismember(jobId,{o.jobs.JobID});
            if ~all(tf);error('JobIds not known.');end
            for i=1:nrFiles
                switch upper(p.Results.type)
                    case 'SH'
                        filename =mslurm.SBATCHFILE;
                    case 'OUT'
                        filename =mslurm.STDOUTFILE;
                    case 'ERR'
                        mslurm.STDERRFILE;
                end
                remoteFile= fullfile(o.remoteStorage,o.jobs(ix(i)).JobName,filename);
                localDir =fullfile(o.localStorage,o.jobs(ix(i)).JobName);
                localFile{i} =fullfile(localDir,filename);
                if ~exist(localFile{i},'file') || p.Results.forceRemote
                    if o.exist(remoteFile,'file')
                        [pth,f,e] = fileparts(remoteFile);
                        o.ssh = scp_get(o.ssh,[f e],localDir,pth);
                    else
                        msg{i} = ['File does not exist: ' remoteName];
                        if nargout <2
                            mslurm.log(msg{i});
                        end
                    end

                end
            end

            if nargout==0
                %% Open in editor
                edit(localFile{:});
            elseif nrFiles==1
                % Return char if possible
                localFile =localFile{1};
                msg  = msg{1};
            end

        end



        function sinfo(o,args)
            % Call sinfo on the cluster. Command line args can be specified
            arguments
                o (1,1) mslurm
                args (1,:) {mustBeText} = '--format "%12P %.5a %.10l %.16F %m %20N"'
            end
            results = o.command(['sinfo ' args]);
            disp(strcat(results))
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
            p.addParameter('jobId',NaN,@(x) isnumeric(x) || iscell(x) || ischar(x));
            p.addParameter('user',o.user,@ischar);
            p.addParameter('units','G',@(x)(strcmp(x,'G') | strcmp(x,'K')));
            p.addParameter('format','jobId,State,ExitCode,jobName,Comment,submit',@ischar);
            p.addParameter('starttime',datetime("now")-1, @isdatetime);
            p.addParameter('endtime',datetime("now") +1, @isdatetime);
            p.addParameter('removeSteps',true,@islogical);
            p.parse(varargin{:});

            if isnumeric(p.Results.jobId)
                if isnan(p.Results.jobId)
                    jobIdStr =  '';
                else
                    jobIdStr =sprintf('%d,',p.Results.jobId(:));
                    jobIdStr = ['--jobs=' jobIdStr];
                end
            else
                if ischar(p.Results.jobId)
                    jobIds = cellstr(p.Results.jobId);
                elseif iscellstr(p.Results.jobId)
                    jobIds = p.Results.jobId;
                end
                jobIdStr = sprintf('--jobs=%s ', jobIds{:});
            end

            if ~isempty(strfind(upper(p.Results.format),'SUBMIT')) %#ok<STREMP>
                format = p.Results.format;
            else
                format = cat(2,p.Results.format,',Submit');
            end

            if isempty(strfind(upper(format),'JOBID')) %#ok<STREMP>
                format = cat(2,format,',jobId');
            end
            if ~isempty(p.Results.user)
                userCmd = [' --user=' p.Results.user];
            else
                userCmd = '';
            end

            % Strange behavior -  if endTime is requested, pending jobs do
            % not show at all. Of course without endTime there is no way to
            % zoom in on a specific set of days. Currently favoring showing
            % more, rather than less.
            cmd  = ['sacct  --parsable2 --format=' format '  ' jobIdStr userCmd ' --starttime=' mslurm.slurmTime(p.Results.starttime) ' --units=' p.Results.units];
            results = o.command(cmd);
            if length(results)>1
                fields = strsplit(results{1},'|');
                for j=1:numel(results)-1
                    thisData = strsplit(results{j+1},'|','CollapseDelimiters',false);
                    data(j) = cell2struct(thisData,fields,2); %#ok<AGROW>
                end

                elapsed = mslurm.matlabTime(data(end).Submit)-mslurm.matlabTime(data(1).Submit);
                elapsed.Format = 'h';
                if p.Results.removeSteps
                    % Remove the jobs that seem to be part of the jobs that I
                    % start (they have jobIDs with .0 or .batch at the end)
                    stay = cellfun(@isempty,regexp({data.JobID},'\.0\>')) & cellfun(@isempty,regexp({data.JobID},'\.batch\>')) ;
                    steps = data(~stay);
                    data = data(stay);
                    jobID={data.JobID};
                    % Sometimes the job can be completed without errors,
                    % but one of the steps that belong to it can fail. (We
                    % had jobs were Matlab was not even started due to some
                    % major node failure). Here we push that fail state to
                    % the parent job that is kept after removing the steps.
                    % This makes it easier to spot such errors in the mslurm
                    % app, for  instance.
                    for s=1:numel(steps)
                        if strcmpi(steps(s).State,'FAILED')
                            dotIx =strfind(steps(s).JobID,'.');
                            parentID = steps(s).JobID(1:dotIx-1);
                            parentIx = strcmp(parentID,jobID);
                            if any(parentIx)
                                data(parentIx).State = 'FAILED';
                            end
                        end
                    end

                end
            else
                mslurm.log(['No sacct information was found for ' jobIdStr '(command = ' cmd ')']);
                data =[];
                elapsed = duration(0,0,0);
                elapsed.Format = 'h';
            end

            %Update internal.
            if nargout==0
                % Sometimes we call sacct to retrieve a different type of
                % information and then we don't want to update the cached
                % job inofrmation
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
        function log(msg,varargin)
            % Writes a message to the command line and adds a clickable link to the relevant line in the m file that generated the message.
            [st] = dbstack('-completenames');
            if length(st)>1
                fun = st(2).name;
                line = st(2).line;
                file = st(2).file;
            else
                fun = 'base';
                file = '';
                line = 0;
            end
            fprintf([msg '\t\t (<a href="matlab:matlab.desktop.editor.openAndGoToLine\t(''%s'',%d);">%s@line %d</a>)\n' ],varargin{:},file,line,fun,line);
        end

        function T = slurmTime(t)
            % Represent a datetime in slurm format.
            T = [char(t,'yyyy-MM-dd') 'T' char(t,'HH:mm:SS')];
        end
        function T= matlabTime(t)
            % Rerpresent a slurm time in Matlab's datetime format.
            if ischar(t)
                tCell= {t};
            else
                tCell =t;
            end
            nrT = numel(tCell);
            for i=1:nrT
                t=tCell{i};
                if length(t)==19
                    vals = t([1:10 12:19]);
                    T(i) = datetime(vals,'InputFormat','yyy-MM-ddHH:mm:SS');                    %#ok<AGROW>
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
            % This function runs on the cluster in response to a call to mslurm.feval on the client.
            % It is not meant to be called directly. See mslurm.feval.
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
            % stored. (and later retrieved by mslurm.retrieve)
            %
            p = inputParser;
            p.addParameter('dataFile','');
            p.addParameter('argsFile','');
            p.addParameter('mFile','');
            p.addParameter('nodeTempDir','');
            p.addParameter('jobDir','')
            p.parse(varargin{:});

            dataMatFile = matfile(p.Results.dataFile); % Matfile object for the data so that we can pass a slice

            % Load a single element of the data cell array from the matfile and
            % pass it to the mfile, together with all the args.

            data = dataMatFile.data(taskNr,:); % Read a row from the cell array
            if isstruct(data)
                data = {data};
            elseif ~iscell(data) %- cannot happen.. numeric is converted to cell in
                %feval and cell stays cell
                error(['The data in ' p.Results.dataFile ' has the wrong type: ' class(data) ]);
            end

            if ~isempty(p.Results.argsFile)  % args may have extra inputs for the user mfile
                load(p.Results.argsFile);
                result = feval(p.Results.mFile,data{:},args); % Pass all cells of the row to the mfile as argument (plus optional args)
            else
                result = feval(p.Results.mFile,data{:}); % No args, pass all cells of the row to the mfile as argument
            end


            % Save the result in the jobDir as 1.result.mat, 2.result.mat
            % etc.
            mslurm.saveResult([num2str(taskNr) '.result.mat'] ,result,p.Results.nodeTempDir,p.Results.jobDir);
        end


        function taskBatchRun(jobID,taskNr,varargin) %#ok
            % This function is a modified version of fevalRun and runs on the cluster in response to
            % a call to mslurm.taskBatch on the client.
            % It is not meant to be called directly. See mslurm.taskBatch.
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
            % stored. (and later retrieved by mslurm.retrieve)
            % 'taskData'  - if an array Job has been submitted then each worker receives a separate slice of data (default)
            %               use '
            try

                p = inputParser;
                addParameter(p,'dataFile','');
                addParameter(p,'argsFile','');
                addParameter(p,'mFile','');
                addParameter(p,'nodeTempDir','');
                addParameter(p,'jobDir','');
                addParameter(p,'totalNrTasks','');
                addParameter(p,'userDir','');
                parse(p,varargin{:});

                p.Results

                %if this is a collate job, then the taskNr will be 0, otherwise
                %it will be the taskNr out of the numbers 1:nrInArray

                if ~isempty(p.Results.dataFile)
                    %preload data and args to pass (slices/subsets) to workers
                    dataMatFile = matfile(p.Results.dataFile) %#ok<NOPRT> % Matfile object for the data so that we can pass a slice
                    dataMatFileInfo = whos(dataMatFile,'data') %#ok<NOPRT>
                    argsMatFile = matfile(p.Results.argsFile) %#ok<NOPRT> % Matfile object for the args so that we can slect slices of of data

                    %data is a cell array and args is a struct which
                    %	contains the field 'tasks', which is a cell array
                    % 	-> pass a subset of data, and a subset of data in args of whatever has the same size as data.
                    %       Both subsets will be selected based on args.tasks{taskNr}

                    fullArgsFile = argsMatFile.args %#ok<NOPRT>
                    argsTemp = rmfield(fullArgsFile,'tasks') %#ok<NOPRT>
                    subTasks = fullArgsFile.tasks{taskNr} %#ok<NOPRT>
                    args = argsTemp %#ok<NOPRT>
                    args.tasks = subTasks %#ok<NOPRT>

                    uDataIdx = unique(args.tasks) %#ok<NOPRT>
                    dataSlice.data = cell(dataMatFileInfo.size) %#ok<NOPRT>
                    for dataColCntr = 1:size(dataSlice.data,2)
                        for uDataCntr = 1:numel(uDataIdx)
                            dataSlice.data{uDataIdx(uDataCntr),dataColCntr} = cell2mat(dataMatFile.data(uDataIdx(uDataCntr),dataColCntr));
                        end
                    end
                    %now get rid of all cells in args that are not needed
                    argsFieldNames = fieldnames(args);
                    for fieldNameCntr = 1:length(argsFieldNames)
                        if isequal(size(args.(argsFieldNames{fieldNameCntr})),size(dataSlice.data))
                            args.(argsFieldNames{fieldNameCntr}) = cell(dataMatFileInfo.size);
                            for colCntr = 1:size(dataSlice.data,2)
                                useIdx =  find(~cellfun(@isempty,dataSlice.data(:,2)));
                                args.(argsFieldNames{fieldNameCntr})(useIdx,colCntr) = fullArgsFile.(argsFieldNames{fieldNameCntr})(useIdx,colCntr);
                            end
                        end
                    end


                    result = feval(p.Results.mFile,dataSlice.data,args); % Pass all cells of the row to the mfile as argument (plus optional args)

                    % Save the result in the jobDir as 1.result.mat, 2.result.mat, etc.
                    mslurm.saveResult([num2str(taskNr) '.result.mat'],result,p.Results.nodeTempDir,p.Results.jobDir);

                else %this means taskNr is 0 and we should collate instead
                    result = mslurm.taskBatchCollate(p.Results.jobDir,'mFile',p.Results.mFile,'totalNrTasks',p.Results.totalNrTasks); % Pass all cells of the row to the mfile as argument (plus optional args)

                    %transfer the collated result to the user's  folder
                    mslurm.saveResult('collated.mat',result,p.Results.nodeTempDir,p.Results.userDir);
                end

            catch theCulprit
                theCulprit %#ok<NOPRT>
                theCulprit.stack(1)
            end
        end

        function result = taskBatchCollate(jobDir,varargin)
            %function result = taskBatchCollate(jobDir,varargin)
            %
            %collate results from calling taskBatch, either by combining them into a struct-array,
            %or by using a function that the user specified for collating their results (see taskBatch).

            try

                p = inputParser;
                addParameter(p,'mFile','');
                addParameter(p,'totalNrTasks',1,@isnumeric);
                parse(p,varargin{:});

                resultFileNames = dir(jobDir);
                resultFileNames = {resultFileNames.name};
                match = contains(resultFileNames,'.result.mat');
                resultFileNames = resultFileNames(match);

                fileNrEndIdx = cell2mat(strfind(resultFileNames,'.result.mat'));

                fileIdx = nan(1,length(resultFileNames));
                tempResult(p.Results.totalNrTasks).result = [];

                for fileCntr = 1:length(resultFileNames)
                    fileIdx(fileCntr) = str2double(resultFileNames{fileCntr}(1:fileNrEndIdx(fileCntr)-1));
                    tempResult(fileIdx(fileCntr)) = load([jobDir '/' num2str(fileIdx(fileCntr)) '.result.mat']);
                    %simplify the output to just 'result(1:nrInArray).fieldnames
                    %instead of result(1:nrInArray).result.fieldnames
                    fieldNames = fieldnames(tempResult(fileIdx(fileCntr)).result);
                    for fieldNameCntr = 1:length(fieldNames)
                        preResult(fileIdx(fileCntr)).(fieldNames{fieldNameCntr}) = tempResult(fileIdx(fileCntr)).result.(fieldNames{fieldNameCntr}); %#ok
                    end
                end


                %decide how tasks should be collated
                if isempty(p.Results.mFile) %the user didn't specify, so the result will be a struct array (result(1:nrInArray).result....)
                    result = preResult;
                else        %the user did specify a function that was translated in to a string of numbers in taskBatch, so now we'll translate it back
                    collateFun = str2num(p.Results.mFile);  %#ok           %str2double doesn't work
                    collateFun = getArrayFromByteStream(uint8(collateFun));

                    if contains(collateFun,'@')  %apparently we are dealing with a string that should be translated to a fucntion(-handle) again
                        collateFun = str2func(collateFun);
                    end

                    result = feval(collateFun,preResult);

                end

            catch theCulprit
                result = theCulprit;
            end
        end


        function fileInFileOutRun(jobID,taskNr,varargin) %#ok<INUSL>
            % This function runs on the cluster in response to a call to mslurm.fileInFileOut on the client.
            % It is not meant to be called directly. call
            % mslurm.fileInFileOut instead.
            %
            % It will run a specified mfile on a file, and save the results
            % in a different file.
            %
            % INPUT:
            % jobID = slurm job id
            % taskNr = The number of this job in the array of jobs. This
            % number is used to pick one item from array of files.
            %
            % Other properties are specified as parm/value pairs
            %
            % 'mfile'  - The Mfile that will be run,.
            % 'datafile'  - File containing the list of files to process.
            % 'argsFile'  - File containing the extra input arguments for
            % the mfile
            % 'nodeTempDir' - folder on the nodes where temporary
            % information can be saved (e.g. /scratch)
            %
            p = inputParser;
            p.addParameter('dataFile','');
            p.addParameter('argsFile','');
            p.addParameter('mFile','');
            p.addParameter('nodeTempDir','');
            p.parse(varargin{:});

            dataMatFile = matfile(p.Results.dataFile); % Matfile object for the data so that we can pass a slice
            if ~isempty(p.Results.argsFile)  % args may have extra inputs for the user mfile
                load(p.Results.argsFile);
            else
                args = {};
            end
            % Load a single element from the file that specifieof the data cell array from the matfile and
            % pass it to the mfile, together with all the args.

            inFile = dataMatFile.inFile(taskNr,1);
            inPath = dataMatFile.inPath(taskNr,1);
            outPath = dataMatFile.outPath;
            outFile = dataMatFile.outFile(taskNr,1);
            filename = fullfile(inPath{1},inFile{1});

            if ~exist(filename,'file')
                error(['File : ' filename ' does not exist']);
            end

            if ~exist(outPath,'dir')
                [success,message] = mkdir(outPath);
                if ~success
                    error(['Failed to create output directory: ' outPath{1} '(' message ')']);
                end
            end

            result = feval(p.Results.mFile,filename,args{:}); % Pass input file and optional args

            % Save the result first locally then scp to outPath
            % puts them back to the main path. Local saves are more reliable
            mslurm.saveResult(outFile{1},result,p.Results.nodeTempDir,outPath);
        end

        function batchRun(jobId,taskNr,varargin)
            % This code runs on the cluster in response to a job submitted
            % with batch. It does some sanity checks, loads any arguments
            % passed to batch, and passes those to the function as a
            % struct.
            p =inputParser;
            p.addRequired('jobId')
            p.addRequired('taskNr')
            p.addParameter('mFile','')
            p.addParameter('argsFile','')
            p.addParameter('nodeTempDir','')
            p.addParameter('jobDir','')
            p.KeepUnmatched = true;
            p.parse(jobId,taskNr,varargin{:});

            p.Results


            if ismember(exist(p.Results.mFile),[2 3 5 6 ]) %#ok<EXIST>  % Executable file
                mslurm.log('%s batch file found\n',p.Results.mFile)
                if isempty(p.Results.argsFile)
                    % Run as script (workspace will be saved to results
                    % file).
                    mslurm.log('Calling %s without input arguments. \n',p.Results.mFile);
                    eval(p.Results.mFile);
                else
                    % It is a function, pass arguments struct
                    if exist(p.Results.argsFile,"file")
                        mslurm.log('%s args file found\n',p.Results.argsFile)
                        load(p.Results.argsFile,'args');
                    else
                        error('% argsFile not found.',p.Results.argsFile);
                    end
                    % A function, pass the input args as a struct
                    mslurm.log('Calling %s with %d input arguments (%s). \n',p.Results.mFile,numel(fieldnames(args)),strjoin(fieldnames(args),'/'));
                    % Output of the function will be saved to results
                    nout =nargout(p.Results.mFile);
                    result = cell(1,nout);
                    [result{:}]= feval(p.Results.mFile,args);      %#ok<NASGU>
                end
            else
                error('%s does not exist. Cannot run this task.',p.Results.mFile)
            end

            %% Save the results
            % Make a struct with all variables in teh workspace
            ws = whos;
            vars = struct;
            for i = 1:length(ws)
                vars.(ws(i).name)= eval(ws(i).name); % Store the variable value
            end
            % Save the result in the jobDir as 1.result.mat, 2.result.mat
            mslurm.saveResult([num2str(p.Results.taskNr) '.result.mat'] ,vars,p.Results.nodeTempDir,p.Results.jobDir);
        end



        function saveResult(filename,result,tempDir,jobDir)
            % Save a result first on a tempDir on the node, then copy it to
            % the head node, using scp. Used by mslurm.run
            [p,f,e] = fileparts(filename);
            if isempty(e) % Force an extension
                e ='.mat';
                filename = [p f e];
            end
            fName =fullfile(tempDir,filename);
            save(fName,'result','-v7.3');

            % Copy to head
            scpCommand = ['scp ' fName ' ' jobDir];
            tic;
            mslurm.log(['Transferring data back to head node: ' scpCommand]);
            [scpStatus,scpResult]  = system(scpCommand);
            if iscell(scpResult) ;scpResult = char(scpResult{:});end

            % Warn about scp errors
            if scpStatus~=0
                scpResult %#ok<NOPRT>
                mslurm.log(['SCP of ' fName ' failed' ]); % Signal error
            elseif ~isempty(scpResult)
                mslurm.log(scpResult)
            end

            [status,result]  = system(['rm  ' fName]);
            if status~=0
                mslurm.log(['rm  of ' fName ' failed'  ]); % Warning only
                result %#ok<NOPRT>
            end

        end

        function me = MException(code,message,varargin)
            if nargin <2
                message = MException.last.message;
            end
            if ~isnumeric(code) || code <1 || code >255
                code  %#ok<NOPRT>
                error('The code for an exception should be a number between 0 an 255' );
            end
            me = MException(sprintf('mslurm:e%d',code),message,varargin{:});
        end

        function exit(me)
            if isnumeric(me) && me==0
                % All is well. Exit with code ==0;
                exit(0);
            else
                % User code generated an exception. Translate to an error code for
                % slurm and put some messages in the log.
                if strcmpi(extractBefore(me.identifier,':'),'mslurm')
                    % Exception generated with mslurm.MException, the number
                    % after :e is the exit code
                    code = str2double(extractAfter(me.identifier,':e'));
                else
                    % A different kind of exception, use the generic exit
                    % error code. of 1
                    code = 1;
                end
                % Output callstack to help debugging
                mslurm.log('*************Message***************\n')
                mslurm.log('-\n %s \n-\n',me.message );
                mslurm.log('***************Call Stack************\n')
                for i=1:numel(me.stack)
                    mslurm.log('Line %d in %s (%s)\n',me.stack(i).line,me.stack(i).name,me.stack(i).file);
                end
                mslurm.log('************************************\n')
                % Now exit
                exit(code);
            end
        end





        %% Tools to store machine wide preferences

        % Prefs are all strings
        function v =  getpref(pref)
            % Retrieve a mSlurm preference
            arguments
                pref (1,:) {mustBeText} = ''
            end
            if isempty(pref)
                % Show all
                v = getpref(mslurm.PREFGRP);
            else
                % Get a save preferred string value.
                if ispref(mslurm.PREFGRP,pref)
                    v = string(getpref(mslurm.PREFGRP,pref));
                else
                    v = "";
                end
            end
        end

        function setpref(pref,value)
            % Set one or more mslurm preferences. Use this to define the
            % cluster host name, user, identity file and job storage.
            % For instance:
            % mslurm.setpref('IdentityFile','my_ssh_rsa')
            % Those settings will persist across Matlab sessions (but not
            % Matlab versions) and allow you to call mslurm with fewer
            % input arguments.
            arguments
                pref
                value
            end
            if ~ismember(pref,mslurm.PREFS)
                error('mslurm only stores prefs for %s',strjoin(mslurm.PREFS,'/'))
            end
            setpref(mslurm.PREFGRP,pref,value);
        end

        function install()
            % Interactive install -  loop over the prefs to ask for values
            % then set.
            for p = mslurm.PREFS
                value = string(input("Preferred value for " + p + "?",'s'));
                mslurm.setpref(p,value)
            end
        end
    end
end



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
%
% See also mslurmApp for a graphical interface to running jobs.
%
%
% BK - Jan 2015
% June 2017 - Public release.
% Sept 2023 - Update./Rewrite

%#ok<*ISCLSTR>
classdef mslurm < handle

    properties (Constant)
        PREFGRP     string = "mSlurm"  % Group that stores string preferences for slurm
        PREFS       string = ["user","keyfile","host","remoteStorage","localStorage", "matlabRoot","nodeTempDir","mslurmFolder"];
        SBATCHFILE  string = "mslurmSBatch.sh";
        STDOUTFILE  string = "stdout";
        STDERRFILE  string = "stderr";
    end

    properties (SetAccess=public, GetAccess=public)
        %% Installation defaults (read from preferences at construction)
        remoteStorage       string  = ""; % Location on HPC Cluster where scripts and logs will be written
        localStorage        string  = ""; % Location on client where logs and scripts will be written
        host                string  = ""; % Host Address
        user                string  = ""; % Remote user name
        keyfile             string  = ""; % Name of the SSH key file. (full name)
        matlabRoot          string  = ""; % Matlab root on the cluster.
        mslurmFolder        string  = ""; % Folder where this mslurm toolbox is installed on the cluster.

        %% Session defaults (can be overruled when submitting specific jobs).
        nodeTempDir         string  = "";  % The path to a directory on a node that can be used to save data temporarily (e.g. /scratch/)
        startupDirectory    string  = "";  % The directory where matlab will start (-sd command line argument)
        workingDirectory    string  = ""; % The directory where the code will execute (if unspecified, defaults to remoteStorage location)
        addPath             string = ""; % String array of folders that shoudl be added to the path.
        batchOptions        = {}; % Options passed to sbatch (parm,value pairs)
        runOptions          string  = ""; % Options passed to srun
        env                 = ""; % A string array of environment variable names that will be read from the client environemtn and passed to the cluster. To set a cluster environment variable to a specific value,some of these elements can be "VAR=VALUE"

    end

    properties (Dependent,SetAccess=protected)
        pwd         string ;    % Working directory on the SLURM cluster
        nrJobs      double ;    % Number of jobs currently available in the log (i.e. submitted between .from and .to)
        isConnected logical;    % Check that we have an open SSH connection to the cluster.
        failState;              % Current state of each of the jobs in the log.
        failStateName;
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

    end

    %% Public Methods
    methods (Access=public)
        function o = mslurm(pv)
            % Construct a mslurm object.Specify parameter/value pairs for
            % Host, User, keyfile, remoteStorage, localStorage,
            % matlabRoot, nodeTempDir,and mslurmFolder.
            % Parameters that are not specified use the
            % preferences stored per machine (see mslurm.install)
            arguments
                pv.host (1,1) string = mslurm.getpref('host');
                pv.user (1,1) string = mslurm.getpref('user');
                pv.keyfile (1,1) string =mslurm.getpref('keyfile');
                pv.remoteStorage (1,1) string = mslurm.getpref('remoteStorage');
                pv.matlabRoot (1,1) string = mslurm.getpref('matlabRoot');
                pv.localStorage (1,1) string = mslurm.getpref('localStorage');
                pv.nodeTempDir (1,1) string = mslurm.getpref('nodeTempDir');
                pv.mslurmFolder (1,1) string = mslurm.getpref('mslurmFolder');
            end

            % Add the SSH2 submodule to the search path (skipped if the user
            % already has ssh2 on the path.)
            if isempty(which('ssh2'))
                here =fileparts(mfilename('fullpath'));
                addpath(fullfile(here,'matlab-ssh2','ssh2'));
            end
            ssh2Install = fileparts(which('ssh2'));
            jarName = strrep(fullfile(ssh2Install,'ganymed-ssh2-build250','ganymed-ssh2-build250.jar'),'\','/');
            if ~exist(jarName,"FILE")
                error('Could not load the SSH2 java file. Please check your matlab-ssh2 installation.')
            end
            javaaddpath(jarName);
            % Setup the object with saved prefs, overruled by session specific
            % input arguments to the constructor
            o.host= pv.host;
            o.user = pv.user;
            o.keyfile = pv.keyfile;
            o.remoteStorage = pv.remoteStorage;
            o.matlabRoot = pv.matlabRoot;
            o.localStorage = pv.localStorage;
            o.nodeTempDir  = pv.nodeTempDir;
            o.mslurmFolder= pv.mslurmFolder;
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
            % Show the fairshare score of the uer (or any user)
            % 1= good, 0= bad
            arguments
                o (1,1) mslurm
                usr (1,1) string  =o.user
            end
            v = o.command("sshare --noheader --Users " +  usr + " --format=''FairShare''");
            v= v{1}; % First line is user, second is 'general'?
            v = str2double(v);
        end

        function gitpull(o,folders,pv)
            % Convenience function to pull a remote git repo from the
            % origin (typially used to sync local code development with
            % remote execution). Submodules can also be updated
            % using 'submodules',true
            %
            % INPUT
            % folders  - The folder with a git repo on the cluster. Or a cell
            % array with such folder names.
            % 'submodules' [false] Set this to true to update submodules recursively with git submodule update --recursive
            %                   Note that in this case the folders have to
            %                   be the toplevel of the git repo.
            arguments
                o (1,1) mslurm
                folders (1,:) string
                pv.submodules (1,1) logical = false
            end

            for fldr=folders
                cmd =sprintf('cd %s && git status',fldr);
                isClean = any(contains(o.command(cmd),'nothing to commit'));
                if isClean
                    if pv.submodules
                        % This sumodule update will only work if the folder is the root folder
                        % of the git repo.
                        cmd =sprintf('cwd= ${pwd} && cd %s && git pull origin && git submodule update --recursive && cd $cwd',fldr);
                    else
                        % This will work from anywhere in the repo
                        cmd =sprintf('cwd= ${pwd} && cd %s && git pull origin && cd $cwd',fldr);
                    end
                    result = o.command(cmd);
                    mslurm.log('%s - %s',fldr, strjoin(result,'\n'))
                else
                    mslurm.log('%s has changes on the remote host. Not git pulling\n',fldr)
                end
            end
        end

        function [results,err] = command(o,cmd,varargin)
            % Execute an arbitrary UNIX command on the cluster.
            arguments
                o (1,1) mslurm
                cmd (1,1) string
            end
            arguments (Repeating)
                varargin
            end
            if ~isempty(varargin)
                cmd = sprintf(cmd,varargin{:});
            end
            if ~isempty(o.ssh)
                USESSHERR = nargout('ssh2_command')==3; % If you have the klab SSH fork it returns err messages for debugging. Otherwise keep this as false.
                try
                    if USESSHERR
                        [o.ssh,results,err] = ssh2_command(o.ssh,char(cmd));
                        if ~isempty(err{1})
                            fprintf('The %s command generated the following errors:\n %s',cmd,strjoin(err,'\n'));
                        end
                    else
                        err= {'unknonwn error'};
                        [o.ssh,results] = ssh2_command(o.ssh,char(cmd));
                    end
                catch me
                    if any(ismember({me.stack.name},'mslurm.connect'))
                        % Avoid recursion.
                        rethrow(me)
                    else
                        % Try to reconnect once.
                        mslurm.log('SSH error. Trying to reconnect');
                        connect(o);
                        [o.ssh,results] = ssh2_command(o.ssh,char(cmd));
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
            arguments
                o (1,1) mslurm
                file (1,1) string
                mode (1,1) string {mustBeMember(mode,["FILE" "DIR"])} = "FILE"
            end
            switch mode
                case "FILE"
                    op= 'e';
                case "DIR"
                    op = 'd';
            end
            result = o.command("[ -%s %s ] && echo ''yes''", op , file );
            yesno = strcmpi(result,'yes');
        end

        function result = put(o,filename, remoteDir)
            % Copy a file to the specified remote directory.
            % The remote directory is created if it does not already exist.
            % filename = The full path to the file to be copied
            % remoteDir = the name of the remote directory [o.remoteStorage]
            arguments
                o (1,1) mslurm
                filename (1,1) string
                remoteDir (1,1) string
            end
            if ~exist(filename,"FILE")
                error('Local file %s not found. Cannot copy to cluster.',filename)
            end
            [localPath,filename,ext] = fileparts(filename);
            filename = filename + ext;
            if nargin<3
                remoteDir = o.remoteStorage;
            end
            if ~o.exist(remoteDir,"DIR")
                result = o.command('mkdir ' + remoteDir);
            end
            try
                o.ssh = scp_put(o.ssh,char(filename),char(remoteDir),char(localPath),char(filename));
            catch
                mslurm.log('SSH error. Trying to reconnect');
                connect(o);
                o.ssh = scp_put(o.ssh,char(filename),char(remoteDir),char(localPath),char(filename));
            end
        end

        function get(o,files,localJobDir,remoteJobDir,deleteRemote)
            % Copy specified files from the remoteJobDir to the localJobDir
            arguments
                o (1,1) mslurm
                files
                localJobDir (1,1) string = o.localStorage
                remoteJobDir (1,1) string = o.remoteStorage
                deleteRemote (1,1) logical = false
            end
            if ischar(files)
                files= {files};
            end
            mslurm.log('Starting data transfer...')
            try
                o.ssh = scp_get(o.ssh,files,char(localJobDir),char(remoteJobDir));
            catch
                mslurm.log('SSH error. Trying to reconnect');
                connect(o)
                o.ssh = scp_get(o.ssh,files,char(localJobDir),char(remoteJobDir));
            end
            mslurm.log('Data transfer done.')

            %% cleanup if requested
            if deleteRemote
                mslurm.log('Deleting remote files')
                o.command('cd ' + remoteJobDir );
                for i=1:numel(files)
                    o.command('rm  %s', files{i});
                end
            end
        end

        function connect(o)
            % Connect to the remote HPC cluster
            if ~exist(o.keyfile,"FILE")
                error('The specified SSH key file does not exist: %s ', o.keyfile);
            end
            o.ssh = ssh2_config_publickey(char(o.host),char(o.user),char(o.keyfile),' ');
            o.ssh.command_ignore_stderr = false;

            if ~o.exist(o.remoteStorage,"DIR")
                result = o.command("mkdir %s",  o.remoteStorage); %#ok<NASGU>
            end
            if ~exist(o.localStorage,"DIR")
                mkdir(o.localStorage);
            end
            %o.ssh.autoreconnect = true; % Seems to make connection slower?
            result = o.command("scontrol show config | sed -n ''/^MaxArraySize/s/.*= *//p''");
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
            arguments
                o (1,1) mslurm
                options (1,1) string = ""
            end
            id = string(datetime("now",'Format','sss'));
            filename = "smap.output." +  id + ".txt";
            msg = o.command(" smap %s -c > %s/%s",options, o.remoteStorage,filename);
            try
                o.ssh = scp_get(o.ssh,char(filename),char(o.localStorage),char(o.remoteStorage));
                o.command("rm %s/%s",o.remoteStorage ,filename);
            catch
                msg = {"Failed to perform an smap"};
            end
            localFile = fullfile(o.localStorage,filename);
            if nargout ==0
                mslurm.log(msg{1});
                edit(localFile);
            else
                T = readtable(localFile);
            end
            delete(localFile);
        end

        function results = retrieve(o,tag,pv)
            % Retrieve the results generated by feval or batch from the cluster
            % The output argument is a cell array with the output of
            % the users' mfile. Jobs that failed will have an empty element
            % in the array. The order of results is the same as the order
            % of the data provided to the mfile in the call to mslurm.feval.
            % INPUT
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
            arguments
                o (1,1) mslurm
                tag (1,1) string
                pv.partial (1,1) logical = true
                pv.deleteRemote (1,1) logical = true
                pv.deleteLocal (1,1) logical = true
                pv.checkSacct (1,1) logical = true
            end
            results ={};
            if pv.checkSacct
                o.sacct; % Update accounting
                state = o.failState; %0 = success, -1 = success after failing, -2 = running, >0= number of attempts.
                thisJob = strcmpi(tag,{o.jobs.JobName});
                nrRunning = sum(state(thisJob)==-2);
                nrFailed  = sum(state(thisJob)>0);
                nrSuccess  =sum(ismember(state(thisJob),[0 -1]));

                if nrFailed >0
                    mslurm.log("%d jobs failed",nrFailed);
                end

                if nrRunning >0
                    mslurm.log("%d jobs still running", nrRunning);
                end
                if nrSuccess >0
                    mslurm.log ("%d jobs completed sucessfully",nrSuccess);
                end

                if ~pv.partial && nrRunning >0
                    mslurm.log("%d jobs are still running. Let''s wait a bit...", nrRunning);
                    return
                end
            end

            %% Check what;s available remotely and transfer.
            remoteJobDir = mslurm.unixfile(o.remoteStorage,tag);
            files = o.command("cd %s; ls *.result.mat",remoteJobDir);
            if ~isempty(files{1})
                localJobDir  = fullfile(o.localStorage,tag);
                if ~exist(localJobDir,"DIR")
                    [success,message] = mkdir(localJobDir);
                    assert(success,"Failed to create local directory to store the results (%s)", message);
                end
                get(o,files,localJobDir,remoteJobDir);
            else
                mslurm.log("No files in job directory %s",remoteJobDir);
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
            if pv.deleteRemote
                for i=1:numel(files)
                    o.command("cd %s; rm %s",remoteJobDir,files{i});
                end
            end
            if pv.deleteLocal
                [success,message]= rmdir(localJobDir,'s');
                if ~success
                    mslurm.log("Could not delete the local job directoryv %s (%s)", localJobDir ,message );
                end
            end
        end


        function jobName = remote(o,fun,pv)
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
            arguments
                o (1,1) mslurm
                fun (1,1)
                pv.funData (:,:) =[]
                pv.funParmValue (1,:) cell ={}
                pv.batchOptions (1,:) cell = o.batchOptions
                pv.runOptions (1,1) string = ""
                pv.debug (1,1) logical  = false
                pv.copy (1,1) logical  = false
                pv.nrWorkers (1,1) double = 0
                pv.expressionName (1,1) string ="expression"
                pv.startupDirectory (1,1) string = o.startupDirectory;
                pv.workingDirectory string = o.workingDirectory;
                pv.addPath (:,:) string = o.addPath;
                pv.env (1,1) string = o.env;
                pv.nodeTempDir (1,1) string =  o.nodeTempDir;

            end


            if contains(fun,{'(',';',','})
                % This is an expression not an mfile. Create a temporary
                mfilename =fullfile(tempdir,pv.expressionName + ".m");
                fid = fopen(mfilename,'w');
                fprintf(fid,'%s\n',fun);
                fclose(fid);
                fun =p.Results.expressionName;
                copy = true; % Have to copy
                assert(isempty(pv.funData),'Passing data to an expression is not supported. Use a function for ''fun'' instead')
            else
                mfilename = which(fun);
                copy = pv.copy;
            end

            [local,remote,jobName]= setupJob(o,fun);

            %% Save parm/value pairs that should be passed
            if ~isempty(pv.funParmValue)
                args = pv.funParmValue;
                argsFile = jobName + "_args.mat";
                % Save a local copy of the args
                localArgsFile = fullfile(local,argsFile);
                remoteArgsFile = mslurm.unixfile(remote,argsFile);
                save(localArgsFile,'args');
                % Copy the args file to the cluster
                o.put(localArgsFile,remote);
            else
                remoteArgsFile ='';
            end


            if isempty(pv.funData)
                remoteDataFile = "";
                assert(pv.nrWorkers>0,"nrWorkers must be larger than zero (%d)" ,pv.nrWorkers)
           
            else
                data =pv.funData;
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
                assert(pv.nrWorkers==0 || pv.nrWorkers==size(data,1),"Mismatch between the rows in funData (%d) and the nrWorkers (%d)",size(data,1),pv.nrWorkers)
                pv.nrWorkers = size(data,1);
                % Save a local copy of the input data
                localDataFile = fullfile(local,'input.mat');
                save(localDataFile,'data','-v7.3'); % 7.3 needed to allow partial loading of the data in each worker.
                % Copy the data file to the cluster
                o.put(localDataFile,remote);
                remoteDataFile= mslurm.unixfile(remote,'input.mat');
            end


            if copy
                % Copy the mfile to remote storage and add it to the remote
                % path
                o.put(mfilename,remote);
                pv.addPath = [pv.addPath remote];
            end

            %% Start the jobs
            opts.batchOptions = pv.batchOptions;
            opts.mfile =fun;
            opts.argsFile =remoteArgsFile;
            opts.dataFile= remoteDataFile;
            opts.nodeTempDir = pv.nodeTempDir;
            opts.debug = pv.debug;
            opts.runOptions= pv.runOptions;
            opts.nrWorkers = pv.nrWorkers;
            opts.startupDirectory = pv.startupDirectory;
            opts.addPath = pv.addPath;
            opts = namedargs2cell(opts);
            o.sbatch(jobName,local,remote,opts{:});

        end


        function [jobId,result] = sbatch(o,jobName,local,remote, pv)
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
            % nrWorkers = This function can start Array Jobs on Slurm. This parm
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
            % o.sbatch('jobName','sort','mfile','psort','mfileExtraInput','parmFile','nrWorkers',10);
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
            arguments
                o (1,1) mslurm
                jobName (1,1) string
                local (1,1) string
                remote (1,1) string
                pv.batchOptions (1,:) cell = o.batchOptions
                pv.runOptions (1,1) string = ""
                pv.mFile (1,1) string = ""
                pv.argsFile (1,1) string = ""
                pv.dataFile (1,1) string = ""
                pv.command (1,1) string = ""
                pv.nrWorkers (1,1) double = 0
                pv.debug (1,1) logical  = false
                pv.taskNr (1,1) double  = 0
                pv.startupDirectory (1,1) string = o.startupDirectory;
                pv.workingDirectory string = o.workingDirectory;
                pv.addPath (:,:) string = o.addPath;
                pv.env (1,1) string = o.env;
                pv.nodeTempDir (1,1) = o.nodeTempDir;
            end



            if pv.mFile ~=""              
                extraIn =sprintf("'mFile',\\""%s\\"",'argsFile',\\""%s\\"",'dataFile',\\""%s\\"",'nodeTempDir',\\""%s\\"",'jobDir',\\""%s\\""",pv.mFile,pv.argsFile,pv.dataFile,pv.nodeTempDir,remote);
                if pv.startupDirectory==""
                    sd = "";
                else
                    sd = "-sd " +   pv.startupDirectory;
                end

                addPth =pv.addPath + ":" + o.mslurmFolder;
                addPathStr = sprintf("addpath('%s')",addPth);

                if pv.workingDirectory ==""
                    % Run in remoteStorage directoryc
                    wd = remote;
                else
                    % Run in specified working Directory
                    wd = pv.workingDirectory;
                end
                if pv.nrWorkers>=1
                    runStr = "%s/matlab "  + sd  + " -nodisplay -nodesktop -r ""try;%s;cd '%s';mslurm.remoteHandler($SLURM_JOB_ID,$SLURM_ARRAY_TASK_ID,%s);catch me;mslurm.exit(me);end;mslurm.exit(0);""";
                    run = sprintf(runStr,o.matlabRoot,addPathStr,wd,extraIn);
                else
                    runStr = "%s/matlab " +  sd  + " -nodisplay -nodesktop -r ""try;%s;cd '%s'; mslurm.remoteHandler($SLURM_JOB_ID,%d ,%s);catch me;mslurm.exit(me);end;mslurm.exit(0);""";
                    run = sprintf(runStr,o.matlabRoot,addPathStr,wd,pv.taskNr,extraIn);
                end
            elseif pv.command ~=""
                % The user knows what to do. Run this command as is with srun.
                run = pv.command;
            else
                error('command and mfile cannot both be empty...');
            end


            if pv.nrWorkers>=1
                % Ensure that array job numbers generated by slurm are
                % base-1 (default is base zero)
                batchOpts = cat(2,pv.batchOptions,{'array',['1-' num2str(pv.nrWorkers)]});
                outFile = mslurm.STDOUTFILE + "_%a.out";
            else
                batchOpts = pv.batchOptions;
                outFile = mslurm.STDOUTFILE;
            end
            % Options with empty values are removed.
            empty = find(cellfun(@isempty,batchOpts(2:2:end)));
            batchOpts([2*empty-1 2*empty])= [];
            empty = find(cellfun(@(x) (isstring(x) && x==""),batchOpts(2:2:end)));
            batchOpts([2*empty-1 2*empty])= [];

            batchFile = mslurm.SBATCHFILE;
            fid  =fopen(fullfile(local,mslurm.SBATCHFILE) ,'w'); % no t (unix line endings are needed)
            fprintf(fid,'#!/bin/bash\n');
            batchOpts = cat(2,batchOpts, {'job-name',jobName,'output',outFile,'error',outFile});
            for opt=1:2:numel(batchOpts)
                if isempty(batchOpts{opt+1})
                    fprintf(fid,'#SBATCH --%s\n',batchOpts{opt});
                elseif isnumeric(batchOpts{opt+1})
                    fprintf(fid,'#SBATCH --%s=%d\n',batchOpts{opt},batchOpts{opt+1});
                elseif ischar(batchOpts{opt+1}) || isstring(batchOpts{opt+1})
                    isSpace  = strcmpi(batchOpts{opt+1},' ');
                    if any(isSpace)
                        batchOpts{opt+1}(isSpace) = '';
                        mslurm.log(['Removing spaces from Slurm Batch Option ''' batchOpts{opt} ''' now set to:''' batchOpts{opt+1} '''']);
                    end
                    fprintf(fid,'#SBATCH --%s=%s\n',batchOpts{opt},batchOpts{opt+1});
                else
                    error('sbatch options should be char or numeric');
                end
            end
            % Read the environment, and package it to pass to sbatch.
            % Note that the passed env are additional to the ones
            % already defined on the cluster (and the ones on the
            % cluster take precedence).
            if pv.env ~=""
                isLocalEnv = ~contains(pv.env,"=");
                val = getenv(pv.env(isLocalEnv));
                str = strjoin(strcat(pv.env(isLocalEnv),'=',val),',');
                str = strjoin([str  pv.env(~isLocalEnv)],',');
                fprintf(fid,'#SBATCH --export=ALL,%s\n',str);
            end
            fprintf(fid,'srun %s %s\n',pv.runOptions,run);
            fclose(fid);


            if pv.debug
                mslurm.log ('Debug mode. Nothing will be submitted to SLURM')
                edit (fullfile(local,batchFile));
                jobId=0;result = 'debug mode';
            else
                % Copy the flie to the cluster
                o.put(fullfile(local,batchFile),remote);
                % Start the sbatch
                [result,err] = o.command(sprintf('cd %s ;sbatch %s/%s',remote,remote,batchFile));
                jobId = str2double(regexp(result{1},'\d+','match'));
                if isempty(jobId) || isnan(jobId)
                    mslurm.log("Failed to submit %s ",jobName)
                    result{1}
                    err{1}
                else
                    mslurm.log("Successfully submitted %s (JobID= %d)",jobName, jobId );
                end
            end
        end


        function result = cancel(o,job)
            % Cancel a slurm job by its name or Job ID
            arguments
                o (1,1) mslurm
                job (1,1) string
            end
            allJobs = [o.jobs];
            if any(contains(job,'-'))
                %A job name ;find the corresponding ID first.
                tf= ismember(job,{allJobs.JobName});
            else
                % A job number (xxx_x)
                tf= ismember(job,{allJobs.JobID});
            end
            if ~all(tf)
                fprintf('No matching job found for %s\n',strjoin(job(~tf),'/'))
            end
            jobIds = {allJobs(tf).JobID};
            if numel(jobIds)>0
                cmd = "scancel " +  sprintf("%s ",jobIds{:});
                result = o.command(cmd);
            else
                fprintf('Nothing to cancel.\n')
            end
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
                %TODO rewrite using scontrol requeue jobID_arrayID

            end
        end


        function [T]=jobsTable(o,expression)
            % Often jobs belong together in a group. By using convention
            % jobName =  Job-SubJob, the 'group' (job) and its elements (subjob)
            % can be determined from the jobName. This is used by mslurmApp to make the
            % interaction with the logs less cluttered.
            arguments
                o (1,1) mslurm
                expression (1,1) string = "(?<group>[\w_-]*)-(?<sub>[\w\d\.]*$)"; % Default format  GROUP-SUB
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

        function [localFile,msg] = getFile(o,job,pv)
            % Retrieve logging files from the cluster. This can be the redirected
            % stdout (type =out) or stderr (type=err), or the bash shell script (type =sh)
            % Currently sbatch writes both out and err to the same .out file so the
            % 'err' file has no information.
            % INPUT
            % jobId  = jobID  (xxxx_xx)  or the jobName created by mslurm
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
            arguments
                o (1,1) mslurm
                job (1,:) string
                pv.element (1,:) double =[]
                pv.forceRemote (1,1) logical =true
                pv.type (1,1) string {mustBeMember(pv.type,["OUT" "ERR" "SH"])} = "OUT"
            end

            if any(contains(job,'-'))
                %A job name ;find the corresponding ID first.
                [tf,ix]= ismember(job,{o.jobs.JobName});
            else
                % A job number (xxx_x)
                [tf,ix]= ismember(job,{o.jobs.JobID});
            end

            %% Construct the file name
            % For running jobs we use scontrol for completed jobs sacct.
            % (scontrol only works for running jobs, sacct does not have
            % the comment while the task is running. Strange but true.
            if ~all(tf)
                fprintf('No matching job found for %s\n',strjoin(job(~tf),'/'))
            end
            nrFiles = numel(ix(ix>0));
            localFile= strings(1,nrFiles);
            msg = repmat("",[1 nrFiles]);
            for i=1:nrFiles
                switch pv.type
                    case 'SH'
                        filename =mslurm.SBATCHFILE;
                    case 'OUT'
                        filename =mslurm.STDOUTFILE;
                    case 'ERR'
                        mslurm.STDERRFILE;
                end

                arrayElement = extractAfter(o.jobs(ix(i)).JobID,'_');
                if ~isempty(arrayElement)
                    arrayElement = ['_' arrayElement]; %#ok<AGROW>
                end
                remoteFile= mslurm.unixfile(o.remoteStorage,o.jobs(ix(i)).JobName,filename + arrayElement + ".out");
                localDir =fullfile(o.localStorage,o.jobs(ix(i)).JobName);
                if ~exist(localDir,"DIR")
                    mkdir(localDir)
                end
                [pth,f,e] = fileparts(remoteFile);
                localFile(i) =fullfile(localDir,f+e);
                if ~exist(localFile(i),"FILE") || pv.forceRemote
                    if o.exist(remoteFile,"FILE")
                        o.ssh = scp_get(o.ssh,char(f + e),char(localDir),char(pth));
                    else
                        msg(i) = "File does not exist: " +  remoteFile;
                        if nargout <2
                            mslurm.log(msg(i));
                        end
                    end

                end
            end

            if nargout==0&& sum(localFile~="") >0
                %% Open in editor
                edit(localFile);
            end
        end

        function sinfo(o,args)
            % Call sinfo on the cluster. Command line args can be specified
            arguments
                o (1,1) mslurm
                args (1,1) string = "--format ""%12P %.5a %.10l %.16F %m %20N"""
            end
            results = o.command('sinfo '+ args);
            disp(strcat(results))
        end

        function [data,elapsed] = sacct(o,pv)
            % Retrieve slurm accounting data from the cluster.
            % INPUT
            % List of parameter/value pairs:
            % 'jobId' = A jobID to retrieve info for a specific job.
            %           Defaults to retrieve information for all jobs in the current jobs list.
            %           Nan to retrieve information for all jobs in sacct.
            % 'user' = Restrict to jobs started by this user [o.user]
            % 'format' = Comma separated string of information to retrieve
            %           (see man sacct , and look at the inputs for format.
            %           Defaults to a small subset of fields.)
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

            arguments
                o (1,1) mslurm
                pv.jobId (1,:) string = ""
                pv.user (1,1) string = o.user
                pv.units (1,1) string = "G"
                pv.format (1,1) string = "jobId,State,ExitCode,jobName,Comment,Submit"
                pv.starttime  (1,1) datetime=datetime("yesterday")
                pv.endtime (1,1) datetime =datetime("tomorrow")
                pv.removeSteps (1,1) logical = true
            end

            if pv.jobId == ""
                jobIdStr =  ' ';
            else
                jobIdStr =sprintf('%d,',pv.jobId(:));
            end

            if  ~contains(pv.format,"Submit",'IgnoreCase',true)
                pv.format = pv.format + "Submit" ;
            end

            if ~contains(pv.format,"JOBID",'IgnoreCase',true)
                pv.format = pv.format + "JobId";
            end

            if ~isempty(pv.user)
                userCmd = " --user=" +  pv.user;
            else
                userCmd = ' ';
            end

            % Strange behavior -  if endTime is requested, pending jobs do
            % not show at all. Of course without endTime there is no way to
            % zoom in on a specific set of days. Currently favoring showing
            % more, rather than less.
            cmd  = "sacct  --parsable2 --format=" +  pv.format + jobIdStr + userCmd + " --starttime=" + mslurm.slurmTime(pv.starttime) + " --units=" + pv.units;
            results = o.command(cmd);
            if length(results)>1
                fields = strsplit(results{1},'|');
                for j=1:numel(results)-1
                    thisData = strsplit(results{j+1},'|','CollapseDelimiters',false);
                    data(j) = cell2struct(thisData,fields,2); %#ok<AGROW>
                end

                elapsed = mslurm.matlabTime(data(end).Submit)-mslurm.matlabTime(data(1).Submit);
                elapsed.Format = 'h';
                if pv.removeSteps
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
                mslurm.log("No sacct information found for %s (command= %s)",jobIdStr,cmd);
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


    end

    methods (Access=protected)
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

        function [local,remote,jobName]= setupJob(o,prefix)
            % Internal function to generate a unique jobName and create local
            % and remote folders to store all files associated with this job.
            arguments
                o (1,1) mslurm
                prefix (1,:) string % A name for this job (or group of jobs)
            end
            % A unique id for a job
            id = string(datetime("now",'Format','yy.MM.dd_HH.mm.SS.sss'));
            jobName = prefix  + "-" +  id;
            mslurm.log('Preparing %s. ',jobName);

            local = fullfile(o.localStorage,jobName);

            if ~exist(local,"DIR")
                [success,message] = mkdir(local);
            end

            assert(success,'Failed to create local folder %s (%s)',local,message);
            remote = mslurm.unixfile(o.remoteStorage,jobName);
            if ~o.exist(remote,"DIR")
                o.command("mkdir " +  remote);
            end
        end

    end



    %% Static helper function
    methods (Static)
        function log(msg,varargin)
            %Writes a message to the command line and adds a clickable link to the relevant line in the m file that generated the message.
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
           % fprintf(msg + "\n" ,varargin{:});
            fprintf(msg + "\t\t (%s@line %d</a>)\n" ,varargin{:},file,line);
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
            arguments
                comment (1,1) string
                tag (1,1) string
                value (1,1) string
            end

            if comment==""
                comment = tag + ":" + value;
            else
                comment = comment + ":" +  tag +  ":" + value;
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
            arguments
                comment (1,:) string
                tag (1,1) string
            end
            match = regexp(comment,tag+":(?<value>[\w\d\s]*$)",'names');
            value = repmat("", [1 numel(comment)]);
            hasMatch= ~cellfun(@isempty,match);
            [value(hasMatch)] = cellfun(@(x) (x.value),match(hasMatch));
        end

        function remoteHandler(jobId,taskNr,pv)
            % This function runs on the cluster in response to a call to
            %
            % INPUT:
            % jobID = slurm job id
            % taskNr = The number of this job in the array of jobs. This
            % number is used to pick one item from the data array.
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
            arguments
                jobId (1,1) double
                taskNr (1,1) double
                pv.mFile (1,1) string % The (user) mFile to call
                pv.dataFile (1,1) string =""   % The user provided data (if any)
                pv.argsFile (1,1) string = ""  % Extra inputs to the user mFile
                pv.nodeTempDir (1,1) string
                pv.jobDir (1,1) string
            end
            mslurm.log("JobID %s runs task %d for %s",jobId,taskNr,pv.mFile);

            %% Check whether pv pairs were provided that should be passed  to
            % the user function
            if pv.argsFile ==""
                args = {};
            else
                if exist(pv.argsFile,"file")
                    mslurm.log("%s args file found\n",pv.argsFile)
                    load(pv.argsFile,'args');
                else
                    error("%s argsFile not found.",pv.argsFile);
                end
            end

            %% Check  whether  data should be passed.
            if pv.dataFile == ""
                % No data passed
                passData = false;
            else
                if exist(pv.dataFile,"file")
                    mslurm.log("%s data file found\n",pv.dataFile);
                    dataMatFile = matfile(pv.dataFile); % Matfile object for the data so that we can pass a slice
                    % Load a single element of the data cell aray from the matfile and
                    % pass it to the mfile, together with all the args.
                    data = dataMatFile.data(taskNr,:); % Read a row from the cell array
                    passData =true;
                else
                    error("%s dataFile not found",pv.dataFile);
                end
            end

            %% Check whether the mFile exists and determine function or script
            % then call the mFile with data and/or args
            if ismember(exist(pv.mFile),[2 3 5 6 ]) %#ok<EXIST>  % Executable file
                mslurm.log("%s file found",pv.mFile)
                try
                    nout = nargout(pv.mFile);
                    isFunction= true;
                catch
                    isFunction= false;
                    assert(isempty(data) && isempty(args),'%s is a script, but you are trying to data or parm/value pairs.')
                end
                if isFunction
                    
                    % A function, pass the input args as a struct
                    mslurm.log("Calling %s with %d input arguments (%s).",pv.mFile,numel(args)/2,strjoin(args(1:2:end),'/'));
                    % Output of the function will be saved to results
                    result = cell(1,nout);
                    if passData
                        data
                        [result{:}]= feval(pv.mFile,data,args{:});
                    else
                        [result{:}]= feval(pv.mFile,args{:});
                    end
                else
                    % A script (workspace will be saved to results file).
                    mslurm.log("Calling %s script.",pv.mFile);
                    wsBefore = whos;
                    eval(pv.mFile);
                    % Make a struct with all variables in the workspace
                    wsAfter = whos;
                    out = intersect({wsAfter.name},{wsBefore.name});
                    wsAfter(out)= [];
                    result = struct;
                    for i = 1:length(wsAfter)
                        result.(wsAfter(i).name)= eval(wsAfter(i).name); % Store the variable value
                    end
                end
            else
                error("%s does not exist. Cannot run this task.",pv.mFile)
            end
            %% Save the results
            % Save the result in the jobDir as 1.result.mat, 2.result.mat
            mslurm.saveResult(string(p.Results.taskNr)+ ".result.mat" ,result,pv.nodeTempDir,pv.jobDir);
        end






        function saveResult(filename,result,tempDir,jobDir)
            % Save a result first on a tempDir on the node, then copy it to
            % the head node, using scp. Used by mslurm.run
            arguments
                filename (1,1) string
                result
                tempDir (1,1) string
                jobDir (1,1) string
            end
            [p,f,e] = fileparts(filename);
            if e=="" % Force an extension
                e =".mat";
                filename = p + f + e;
            end
            fName =fullfile(tempDir,filename);
            save(fName,'result','-v7.3');

            % Copy to head
            scpCommand = "scp " + fName + " " + jobDir;
            tic;
            mslurm.log("Transferring data back to head node %s",scpCommand);
            [scpStatus,scpResult]  = system(scpCommand);
            if iscell(scpResult) ;scpResult = string(scpResult{:});end

            % Warn about scp errors
            if scpStatus~=0
                scpResult %#ok<NOPRT>
                mslurm.log("SCP of %sfailed",fName); % Signal error
            elseif ~isempty(scpResult)
                mslurm.log(scpResult)
            end

            [status,result]  = system("rm " + fName);
            if status~=0
                mslurm.log("rm  of  %s  failed ", fName); % Warning only
                result %#ok<NOPRT>
            end

        end

        function me = MException(code,message,varargin)
            arguments
                code
                message = MException.last.message;
            end
            arguments (Repeating)
                varargin
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
                pref (1,1) string = ""
            end
            if pref ==""
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
                pref (1,1) string
                value  (1,1) string
            end
            assert(ismember(pref,mslurm.PREFS),"mslurm only stores prefs for %s",strjoin(mslurm.PREFS,'/'))
            setpref(mslurm.PREFGRP,pref,value);
        end

        function install()
            % Interactive install -  loop over the prefs to ask for values
            % then set.
            fprintf("Installing preferences for mslurm (see Readme.md for details)\n");
            for p = mslurm.PREFS
                currentValue = mslurm.getpref(p);
                if currentValue ==""
                    msg ="Preferred value for " + p + "?";
                else
                    msg = "Preferred value for " + p + " (current value = " + currentValue + " )?";
                end
                value = string(input(msg,'s'));
                if value~=""
                    mslurm.setpref(p,value)
                    fprintf('Set %s to %s\n',p,value);
                else
                    fprintf('No change for %s\n',p);
                end
            end
            fprintf("mslurm installation is complete. \n");

        end


        %% Set the status bar text of the Matlab desktop (from undocumented matlab)
        function status(msg,varargin)
            statusText = sprintf(msg,varargin{:});
            desktop = com.mathworks.mde.desk.MLDesktop.getInstance;     %#ok<JAPIMATHWORKS>
            if desktop.hasMainFrame
                % Schedule a timer to update the status text
                % Note: can't update immediately (will be overridden by Matlab's 'busy' message)
                timerFcn = @(~,~)(desktop.setStatusText(statusText));
                t = timer('TimerFcn',timerFcn, 'StartDelay',0.05, 'ExecutionMode','singleShot');
                start(t);
            else
                disp(statusText);
            end
        end

        function v = unixfile(varargin)
            % Same as fullfile , but forces unix pathseparator ('/')
            v = fullfile(varargin{:});
            v = strrep(v,filesep,'/');
        end
    end
end



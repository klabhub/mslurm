function result = slurmDiagnose(options)
% Utility function to run diagnostics on the server. Run this with
% mslurm.remote and then look by inspecting the logs from the slurmApp or
% with mslurm.getFile.
% The options are (an array of) strings that describe what will be
% done:
%  "basic" : some general diagnostics about the node where Matlab runs
%
% EXAMPLE:
% s.remote("slurmDiagnose","BASIC")
% will perform the basic diagnosis on a single node.
% s.feval("slurmDiagnose",repmat("basic",[1,3]))
% will perform the basic diagnosis on three nodes/workers.
%
% BK - June 2017
arguments 
    options (1,:) string = "BASIC"
end

result = [];  % Most diagnoses don't return values, Just inspect the logfile.

for o=1:numel(options)
    switch upper(options{o})
        case 'BASIC'
            disp(['OS: ' computer]);
            disp (['Working directory: ' pwd]);
            if ispc
            disp (['User: ' getenv('Username')]);
            else
                disp (['User: ' getenv('User')]);
            end
            if ispc
                disp(['Computer:' getenv('COMPUTERNAME')]);
            else
                disp(['Computer:' getenv('HOSTNAME')]);
            end
            
            disp(['Userpath: '  userpath]);
            
           
            %% Version 
            ver
            
            disp('Path')
            path
            
        case 'CONFIG'
            % Type the slurm.conf file in the command prompt (so that is
            % shows up in the log file in Matlab)
            type /etc/slurm/slurm.conf
            
            
            %case xxxx - extend this function here with new options
        otherwise
    end
end



end
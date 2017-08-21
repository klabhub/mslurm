function result = slurmDiagnose(options)
% Utility function to run diagnostics on the server. Run this with
% slurm.feval and then look at the result with slurm.retrieve or inspect
% the logs from the sortGui.
% The options are (a cell array of) strings that describe what will be
% done:
%  'basic' : some general diagnostics about the node where Matlab runs
%
% EXAMPLE:
% s.feval('slurmDiagnose',{'basic'})
% will perform the basic diagnosis on a single node.
% s.feval('slurmDiagnose',repmat({'basic'},[1,3]))
% will perform the basic diagnosis on three nodes/workers.
%
% BK - June 2017

result = [];  % Most diagnoses don't return values, Just inspect the logfile.

if ischar(options)
    options = {options};
end

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
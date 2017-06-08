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

if ischar(options)
    options = {options};
end

for o=1:numel(options)
    switch upper(options{o})
        case 'BASIC'
            disp ('Working directory')
            pwd
            
            disp ('User')
            getenv('Username')
            
            disp('node name')
            getenv('COMPUTERNAME')
            
            disp('Userpath')
            userpath
            
            disp('Path')
            path
            
            
            %case xxxx - extend this function here with new options
        otherwise
    end
end



end
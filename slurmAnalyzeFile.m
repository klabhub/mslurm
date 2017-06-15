function result = slurmAnalyzeFile(filename)
% Example function to demonstrate slurm.fileInFileOut
% 
% When called apporpriately with slurm.feval, this function will be called
% with a single file as its 'filename' input argument. The code below checks that
% this is the case and then does some (nonsensical) analysis on the file
%
% See slurmExample. 

if ~ischar(filename) || ~exist(filename,'file')
    error('slurmAnalyzeFile requires the name of a single, existing file as its input');
end

% The analysis...
load(filename)
result = whos; % Our analysis is to simply list what is in this file...

end
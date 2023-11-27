function result = analyzeFile(filename)
% Example function to demonstrate 
% 
% When called apporpriately with mslurm.remote, this function will be called
% with a single file as its 'filename' input argument. The code below checks that
% this is the case and then does some (nonsensical) analysis on the file
%
% See Also tutorial.mlx
arguments
    filename (1,1) string
end

if ~exist(filename,'file')
    error('analyzeFile requires the name of a single, existing file as its input');
end

% The analysis...
load(filename) %#ok<LOAD>
result = whos; % Our analysis is to simply list what is in this file...

end
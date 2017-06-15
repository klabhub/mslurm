function result = slurmAnalyzeRt(data)
% Example function to demonstrate slurm.feval
% 
% When called apporpriately with slurm.feval, this function will be called
% with a single struct as its 'data' input argument. The code below checks that
% this is the case and the calculates the mean reaction time in the data.
%
% See slurmExample. 

if ~isstruct(data) || ~numel(data)==1 || ~isfield(data,'rt')
    error('slurmAnalyzeRt requires a single data struct as its input');
end

% The analysis. 
result  = mean(rt);

end
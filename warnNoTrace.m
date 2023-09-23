function warnNoTrace(msg,varargin)
% Same as warning, but without the (often confusing) backtrace
% and a clickable link to the relevant line in the m file that generated the warning.

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

state=warning;
warning('backtrace','off')
warning([msg '(<a href="matlab:matlab.desktop.editor.openAndGoToLine(''%s'',%d);">%s@line %d</a>) ' ],varargin{:},file,line,fun,line);
warning(state);
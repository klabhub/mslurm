function varargout = slurmGui(varargin)
% slurmGui - A graphical interface to monitor SLURM jobs from Matlab.
% Hover your mouse over the main panel in the Gui to read some
% instructions.
%
% BK -March 2015
% June 17. Public
% 
% See also @slurm 

% Edit the above text to modify the response to help slurmGui

% Last Modified by GUIDE v2.5 21-May-2017 12:53:13

% Begin initialization code - DO NOT EDIT
gui_Singleton = 1;
gui_State = struct('gui_Name',       mfilename, ...
    'gui_Singleton',  gui_Singleton, ...
    'gui_OpeningFcn', @slurmGui_OpeningFcn, ...
    'gui_OutputFcn',  @slurmGui_OutputFcn, ...
    'gui_LayoutFcn',  [] , ...
    'gui_Callback',   []);
if nargin && ischar(varargin{1})
    gui_State.gui_Callback = str2func(varargin{1});
end


if nargout
    [varargout{1:nargout}] = gui_mainfcn(gui_State, varargin{:});
else
    gui_mainfcn(gui_State, varargin{:})
end
% End initialization code - DO NOT EDIT
end

% --- Executes just before slurmGui is made visible.
function slurmGui_OpeningFcn(hObject, ~, handles, varargin)
% This function has no output args, see OutputFcn.
% hObject    handle to figure
% eventdata  reserved - to be defined in a future version of MATLAB
% handles    structure with handles and user data (see GUIDATA)
% varargin   command line arguments to slurmGui (see VARARGIN)


%% Store some useful information.
handles.output = hObject;
if numel(varargin)<1 || ~isa(varargin{1},'slurm')
    error('Please provide a slurm object as input to slurmGui');
end
handles.slurm = varargin{1}; % Access to the slurm object
handles.parms.colors =   {'Green','Blue','Orange','Red','Purple'};   
handles.current.groupIx = []; % Will be filled later.
handles.current.subs = {};
handles.current.groups = {};
handles.current.jobsPerGroup = {};
handles.current.selection = [];
handles.current.failState = [];
handles.current.failStateColor = {};
handles.current.groupFailState = [];

%% Add a date control based on JIDE. The callback
com.mathworks.mwswing.MJUtilities.initJIDE;
jPanel = com.jidesoft.combobox.DateChooserPanel;
set(handles.datePanel,'Units','pixel');
[handles.dates] = javacomponent(jPanel,get(handles.datePanel,'Position'),handles.figure1);
set(handles.dates,'ShowTodayButton',false,'ShowNoneButton',false,'ShowWeekNumber',false);
jModel = handles.dates.getSelectionModel;  % a com.jidesoft.combobox.DefaultDateSelectionModel object
jModel.setSelectionMode(jModel.SINGLE_INTERVAL_SELECTION);
hModel = handle(jModel, 'CallbackProperties');
set(hModel, 'ValueChangedCallback',@(x,y)(slurmGui('dateSelectionCallback',handles.dates,handles.figure1)));

%% Make the table sortable
obj = findjobj(handles.sub);
if ~isempty(obj )
    handles.subJava  = obj.getViewport.getView;
    handles.subJava.setAutoResizeMode(handles.subJava.AUTO_RESIZE_SUBSEQUENT_COLUMNS);
    % Now turn the JIDE sorting on
    handles.subJava.setSortable(true);
    handles.subJava.setAutoResort(true);
    handles.subJava.setMultiColumnSortable(true);
    handles.subJava.setPreserveSelectionsAfterSorting(true);
    set(handles.subJava,'SelectionBackground',java.awt.Color(0.9,0.9,0.9))
end

obj  = findjobj(handles.job);
set(obj.getViewport.getView,'SelectionBackground',java.awt.Color(0.9,0.9,0.9));

guidata(hObject,handles);
refresh(handles); % Pull fresh inforamtion from SLURM
end


function refresh(handles)
% Function that connects to SLURM to collect the logs and then upodates the
% interface
set(handles.figure1,'Pointer','watch');


if ~isempty(handles.current.groups)
    oldGroupSelection = get(handles.job,'value');
    oldGroupSelection  = handles.current.groups{oldGroupSelection};
else
    oldGroupSelection =[];
end
drawnow;

%if handles.slurm.
%% Collect from SLURM and update display
handles.slurm.sacct('format','jobId,State,ExitCode,jobName,Comment,submit,elapsed,NodeList,end'); % Update from Slrum
if handles.slurm.isConnected
    set(handles.figure1,'Name',['SLURM GUI - Connected to ' handles.slurm.host ' as ' handles.slurm.user]);
else
    set(handles.figure1,'Name','SLURM GUI - Not Connected');
end
set(handles.jobsLabel,'String',['Total:' num2str(handles.slurm.nrJobs) ' jobs']);

%% Group jobs if possible
% Show jobs as groups (convention : groupname-subname is hte JobName in
% slurm, but this can be changed in the slurm class.
if handles.slurm.nrJobs >0
    [handles.current.groups,handles.current.groupIx,handles.current.subs,handles.current.jobsPerGroup] = jobGroups(handles.slurm);
    if length(handles.current.subs) ~=handles.slurm.nrJobs
        error('Not all jobs are shown... job grouping is not working?');
    end
    handles.current.failState = handles.slurm.failState;
    
    nrGroups =numel(handles.current.groups);
    groupString = cell(1,nrGroups);
    for group=1:nrGroups
        try
        jobsInGroup = [handles.current.jobsPerGroup{group}];        
        failState = handles.current.failState(jobsInGroup);
        failStateColor = nan(size(failState));
        failStateColor(failState==0) =1; % Green (completed success)
        failStateColor(failState==-1) =2; % Blue Failed but retried 
        failStateColor(failState==-2) =3; % Orange: Running 
        failStateColor(failState>0 & failState<=10) =4; % Red: Failed: 
        failStateColor(failState>10 ) =5; % Purple : Failed too often
        handles.current.failStateColor{group} = failStateColor;
        handles.current.groupFailState(group) = max(failStateColor);        
        groupString{group} = sprintf('<HTML><font color=''%s''>%s</font></HTML>',handles.parms.colors{handles.current.groupFailState(group)},handles.current.groups{group});
        catch
            groupString{group} = 'ungroupable';
        end
    end
    
    
    set(handles.job,'string',groupString,'value',nrGroups);
    
else
    set(handles.job,'string','','value',0);
end

if ~isempty(oldGroupSelection)
    % Restore
    selection = find(ismember(handles.current.groups,oldGroupSelection));
    if ~isempty(selection)
        set(handles.job,'Value',selection)
    end
end
%% Update dependencies
update(handles);
set(handles.figure1,'Pointer','arrow');
drawnow

end


function update(handles)
%Update the interface after a click
set(handles.figure1,'Pointer','watch');
drawnow;
group = get(handles.job,'value');
if any(group>0)
    % Construc the data cell array to be passed to the uitable.
     jobsInGroup = [handles.current.jobsPerGroup{group}];
        jobs = handles.slurm.jobs(jobsInGroup);
        nrJobs = length(jobsInGroup);
       subs = handles.current.subs(jobsInGroup);
    stateStrings = cell(nrJobs,1);
    colors = [handles.current.failStateColor{group}];
    for i=1:nrJobs
        stateStrings{i} = sprintf('<HTML><font color=''%s''>%s</font></HTML>',handles.parms.colors{colors(i)},jobs(i).State);        
    end
    endTime = slurm.matlabTime({jobs.End});
    submitTime = slurm.matlabTime({jobs.Submit});
 
    totalTime = cellstr(datestr(endTime-submitTime,'HH:MM:SS'));
    out = endTime==0 ;
    [totalTime{out}] = deal('NaN');

    elapsed = cellstr(datestr({jobs.Elapsed},'HH:MM:SS'));
    date =cellstr(datestr(submitTime,'dd-mmm-yy'));       
    submitTime = cellstr(datestr(submitTime,'HH:MM'));
    data = {subs{:};stateStrings{:};jobs.ExitCode;date{:};submitTime{:};elapsed{:};totalTime{:};jobs.JobID;jobs.NodeList}';
    data = flipud(data);
    set(handles.sub,'Data',data)
    set(handles.subLabel,'string',['Group: ' num2str(nrJobs) ' jobs - ' num2str(sum(strcmpi('Completed',{jobs.State}))) ' completed. ' num2str(sum(strcmpi('Running',{jobs.State}))) ' running, and ' num2str(sum(strcmpi('Pending',{jobs.State}))) ' pending.']);
else
    set(handles.sub,'Data',{})
    set(handles.subLabel,'string','About nothing');    
end
handles.current.selection = [];
guidata(handles.figure1,handles);
set(handles.figure1,'Pointer','arrow');
drawnow
end

function dateSelectionCallback(hObject,hFigure) 
% Called on selecting one or more dates. - Updates logs from SLRUM
handles= guidata(hFigure);
dates = hObject.getSelectionModel.getSelectedDates;
persistent oldDates

refreshIt = isempty(oldDates) || length(oldDates) ~= length(dates) || ~strcmp(dates(1),oldDates(1)) || ~strcmp(dates(end),oldDates(end));
oldDates = dates;

if refreshIt
    
    handles.slurm.from = datenum(dates(1).getYear+1900,dates(1).getMonth+1,dates(1).getDate);
    if numel(dates)>1
        handles.slurm.to = datenum(dates(end).getYear+1900,dates(end).getMonth+1,dates(end).getDate+1);
    else
        handles.slurm.to = datenum(now+1);
    end
    refresh(handles);
end
end

% --- Outputs from this function are returned to the command line.
function varargout = slurmGui_OutputFcn(~, ~, handles)
% varargout  cell array for returning output args (see VARARGOUT);
% hObject    handle to figure
% eventdata  reserved - to be defined in a future version of MATLAB
% handles    structure with handles and user data (see GUIDATA)

% Get default command line output from handles structure
varargout{1} = handles.output;
end

% --- Executes during object creation, after setting all properties.
function job_CreateFcn(hObject, ~, ~)
% hObject    handle to job (see GCBO)
% eventdata  reserved - to be defined in a future version of MATLAB
% handles    empty - handles not created until after all CreateFcns called

% Hint: listbox controls usually have a white background on Windows.
%       See ISPC and COMPUTER.
if ispc && isequal(get(hObject,'BackgroundColor'), get(0,'defaultUicontrolBackgroundColor'))
    set(hObject,'BackgroundColor','white');
end
end



% --- Executes on key press with focus on job and none of its controls.
function job_KeyPressFcn(~, eventdata, handles) %#ok<*DEFNU>
% hObject    handle to job (see GCBO)
% eventdata  structure with the following fields (see MATLAB.UI.CONTROL.UICONTROL)
%	Key: name of the key that was pressed, in lower case
%	Character: character interpretation of the key(s) that was pressed
%	Modifier: name(s) of the modifier key(s) (i.e., control, shift) pressed
% handles    structure with handles and user data (see GUIDATA)

group = get(handles.job,'value');
if any(group>0)
    jobsInGroup = [handles.current.jobsPerGroup{group}];
    jobs = handles.slurm.jobs(jobsInGroup);
    switch eventdata.Key
        case 'r'
            % Retry all failed jobs in this group
            failState = handles.current.failState(jobsInGroup);
            failedJobs = jobs(failState>0);
            nrJobs = length(failedJobs);
            if nrJobs>0
                handles.slurm.retry('jobId',{failedJobs.JobID});
                refresh(handles);
            else
                msgbox('No failed jobs for this group','Nothing to do?');
            end
        case 'c'
            canBeCancelled = ismember({jobs.State},{'RUNNING','PENDING'});
            jobs = jobs(canBeCancelled);            
            jobIDs = {jobs.JobID};  % Store all in a cell
            % Match with something like JobID_[From-To], which is the
            % format used for array jobs
            match = regexp(jobIDs,'(?<jobID>\d+)_\[(?<from>\d+)-(?<to>\d+)\]','names');
            nrInArray = 0;
            nrArrayJobs=0;
            out = [];
                for i=1:numel(match)
                    if ~isempty(match{i})
                        jobIDs{i} = match{i}.jobID; % Replace with actual jobID to cancel the entire array of jobs.
                        if i<numel(match)
                            % By canceling the parent array job all
                            % children jobs will be canceled automatically. We remove
                            % them from the list that is sent to scancel
                            out = [out i+find(strncmpi([jobIDs{i} '_'],jobIDs((i+1):end),numel(jobIDs{i})+1))];                             %#ok<AGROW>
                        end
                        nrArrayJobs = nrArrayJobs +1;
                        nrInArray = nrInArray + (str2double(match{i}.to)-str2double(match{i}.from)+1);
                    end
                end       
            jobIDs(out) = [];
            nrJobs = length(jobIDs);            
            answer = questdlg(sprintf('Do you want to cancel %d jobs (including %d array jobs with %d sub-jobs?',nrJobs,nrArrayJobs,nrInArray),'Please Confirm','Yes');
            if strcmpi(answer,'yes')
               
                % Tell slurm to cancel all.
                handles.slurm.cancel('jobId',jobIDs);
            end
            refresh(handles);
        otherwise
            % Ignore key
    end
end
end

% --- If Enable == 'on', executes on mouse press in 5 pixel border.
% --- Otherwise, executes on mouse press in 5 pixel border or over job.
function job_ButtonDownFcn(hObject, eventdata, handles) %#ok<*INUSD>
% hObject    handle to job (see GCBO)
% eventdata  reserved - to be defined in a future version of MATLAB
% handles    structure with handles and user data (see GUIDATA)


end

% --- Executes on selection change in job.
function job_Callback(~, ~, handles)
% hObject    handle to job (see GCBO)
% eventdata  reserved - to be defined in a future version of MATLAB
% handles    structure with handles and user data (see GUIDATA)

update(handles)
end



% --- Executes when selected cell(s) is changed in sub.
function sub_CellSelectionCallback(~, eventdata, handles)
% hObject    handle to sub (see GCBO)
% eventdata  structure with the following fields (see MATLAB.UI.CONTROL.TABLE)
%	Indices: row and column indices of the cell(s) currently selecteds
% handles    structure with handles and user data (see GUIDATA)
handles.current.selection = eventdata.Indices;
guidata(handles.figure1,handles);
end




% --- Executes on button press in refresh.
function refresh_Callback(~, ~, handles)
% hObject    handle to refresh (see GCBO)
% eventdata  reserved - to be defined in a future version of MATLAB
% handles    structure with handles and user data (see GUIDATA)
refresh(handles)
end


% --- Executes on key press with focus on sub and none of its controls.
function sub_KeyPressFcn(~, eventdata, handles)
% hObject    handle to sub (see GCBO)
% eventdata  structure with the following fields (see MATLAB.UI.CONTROL.TABLE)
%	Key: name of the key that was pressed, in lower case
%	Character: character interpretation of the key(s) that was pressed
%	Modifier: name(s) of the modifier key(s) (i.e., control, shift) pressed
% handles    structure with handles and user data (see GUIDATA)

if ~isempty(handles.current.selection)
    % Deal with sorted tables
    jRows = handles.subJava.getSelectedRows;
    nrRows= numel(jRows);
    sortedSelection = nan(1,nrRows);
    for i=1:nrRows
        sortedSelection(i) = handles.subJava.getActualRowAt(jRows(i))+1;
    end
    jobId = char(handles.sub.Data{sortedSelection,end});
    switch eventdata.Key
        case 'e'
            % Read error file
            handles.slurm.logFile(jobId,'type','err')
        case 'o'
            % Read output file
            handles.slurm.logFile(jobId,'type','out')
         case 's'
             % Read bash script 
             handles.slurm.logFile(jobId,'type','sh')
        case 'r'
            % Retry job
            handles.slurm.retry('jobId',jobId);
            %refresh(handles);
        case 'c'       
            % Cancel job
            handles.slurm.cancel('jobId',jobId);
            refresh(handles);
        case 't'
            % Technical details
            frmt = 'AveCPU,AvePages,AveRSS,AveVMSize,cputime,maxRSS,maxVMSize,nodelist,State';
            %jobId = strsplit(jobId,'_');% In case this is an arry job
            slurmJobDetails = handles.slurm.sacct('jobId',jobId,'format',frmt,'removeSteps',false) %#ok<NOPRT>            
            assignin('base','slurmJobDetails',slurmJobDetails)            
            warning([ num2str(numel(slurmJobDetails)) ' technicals details have been assigned to ''slurmJobDetails''']);
        case 'g'
            % Retrieve the data procuded by an slurm.feval job
            ix =strcmpi(jobId,{handles.slurm.jobs.JobID});
            tag = handles.slurm.jobs(ix).JobName;
            data = handles.slurm.retrieve(tag);
            assignin('base','data',data)
            warning([ num2str(numel(data)) ' results of the job have been assigned to ''data''']);
        case 'm'
            % Retrieve an smap and show it in the editor
            handles.slurm.smap;       
        otherwise
            % Unkown key, ignore
    end
end
end


function sub_CreateFcn(hObject, eventdata, handles)
set(hObject,'TooltipString',strcat('<html> This GUI shows the accounting log of a SLURM HPC Cluster. <BR> <HR>  Start by selecting a date (or a range of dates). <BR> ', ...
                             'This will contact SLURM and show the jobs currently in the account log. <BR>  The refresh button updates this information (by contacting SLURM again) <BR>  <HR> ',...
                            'The panel on the left shows groups of jobs that belong together. <BR>  This relies on a convention for job-naming used by the @slurm class. <BR>',...
                            'The panel on the right displays jobs in the selected group that ran <BR> on the cluster in the selected date range. Click on the headers to sort the jobs.<BR>',...
                            '<HR> Color is used to indicate job state:  <br> <font color=00ff00>Completed Successfully</font> <br> <font color=ddaa00>Running now</font> <br>',...
                            '<font color=0000ee>Failed previously but ok now.</font> <br> <font color=ee0000>Failed</font> <br> <font color=ee00ee>Failed more than 10 times</font> ',...
                            '<br> <br> The color of a group indicates the state of the worst job in the group. <BR> Any group with green or blue status therefore completed successfully (eventually). <HR> ',...
                            'Use keys to control jobs: <br> <b>o</b> opens a file showing Matlab command line output <br> <b>e</b> opens a file showing cluster and Matlab errors <br> ',...
                            '<b>r</b> retries selected jobs (or all failed jobs when a group is selected) <br> <b>c</b> cancels selected jobs (or all running jobs when a group is selected)<br> ',...
                            '<br> <b>g</b> retrieves the results of slurm.feval jobs (and assigns them to the ''data'' variable in the base workspace.br> ',...
                            '<br> <b>m</b> opens a file in the editor that shows the current usage of the cluster (smap) ',...
                            '<br><b>s</b> opens the sbatch .sh shell file in the editor.',...
                            '<br><b>t</b> shows technical details about a job.',...                            
                            '<HR>  BK -March 2015,2018 </html> '));
                        
                        
                        
end

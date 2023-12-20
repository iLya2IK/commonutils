{
 SortedThreadPool
   Modul fo async execution of clients jobs with ranking
   Based on
   Copyright (C) 2011 Maciej Kaczkowski / keit.co

   Added jobs ranking
   Copyright (C) 2020 Ilya Medvedkov

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
}


unit SortedThreadPool;

{$mode objfpc}{$H+}

interface

uses
  AvgLvlTree, ECommonObjs, OGLFastList, Classes, SysUtils;

type
  TSortedThreadPool = class;

  { TLinearJob }

  TLinearJob = class
  private
    FNeedToRestart : Boolean;
    FHoldOnValueMs : Cardinal;
    FHoldStart     : QWord;
  public
    constructor Create;
    //set NeedToRestart = true if need to restart job
    //after execution
    procedure RestartJob(aHoldDelay : Cardinal; const aHoldStart : QWord);
    property NeedToRestart : Boolean read FNeedToRestart write FNeedToRestart;
    property HoldOnValueMs : Cardinal read FHoldOnValueMs write FHoldOnValueMs;
    function IsReady(const TS: QWord): Cardinal;
    procedure Execute; virtual; abstract;
  end;

  { TSortedJob }

  TSortedJob = class(TLinearJob)
  private
    FScore : Cardinal;
  public
    constructor Create(aScore : Cardinal); virtual;
    procedure UpdateScore; virtual;
    property Score : Cardinal read FScore write FScore;
  end;

  TPoolThreadKind = (ptkLinear, ptkSorted);

  { TThreadPoolThreadKind }

  TThreadPoolThreadKind = class(TNetCustomLockedObject)
  private
    FValue : TPoolThreadKind;
    function GetValue: TPoolThreadKind;
    procedure SetValue(AValue: TPoolThreadKind);
  public
    constructor Create(AValue : TPoolThreadKind);
    property Value : TPoolThreadKind read GetValue write SetValue;
  end;

  TJobToJobWait = record
    DefaultValue : Integer;
    AdaptMin : Word;
    AdaptMax : Word;
  end;

  { TThreadJobToJobWait }

  TThreadJobToJobWait = class(TNetCustomLockedObject)
  private
    FValue : TJobToJobWait;
    function GetValue: TJobToJobWait;
    procedure SetValue(AValue: TJobToJobWait);
  public
    constructor Create(AValue : TJobToJobWait);
    procedure AdaptDropToMin(var AValue: Integer);
    procedure AdaptToDec(var AValue : Integer);
    procedure AdaptToInc(var AValue : Integer);
    function IsEqual(AValue : TJobToJobWait) : Boolean;
    property Value : TJobToJobWait read GetValue write SetValue;
  end;

  TOnRefreshJob = procedure (j : TLinearJob; const delta : Cardinal) of object;
  TOnRebornJob = procedure (j : TLinearJob) of object;

  { TWaitingCollection }

  TWaitingCollection = class(specialize TFastBaseCollection<TLinearJob>)
  private
    FDelta : Cardinal;
  public
    constructor Create(dt : Cardinal);

    procedure AddJob(j : TLinearJob);
    procedure DeleteJob(i : integer);

    procedure Refresh(const TS: QWord; cb : TOnRefreshJob);
  end;

  { TWaitingPool }

  TWaitingPool = class
  private
    F1msPool : TWaitingCollection;
    F10msPool : TWaitingCollection;
    F100msPool : TWaitingCollection;
    F1000msPool : TWaitingCollection;
    FOnRebornJob: TOnRebornJob;

    FTS1ms, FTS10ms, FTS100ms, FTS1000ms : QWord;

    procedure DoOnRefreshJob(j : TLinearJob; const delta : Cardinal);
  public
    constructor Create;
    destructor Destroy; override;

    procedure AddJob(j : TLinearJob; timeout : Cardinal);

    procedure Refresh(const TS: QWord);

    property OnRebornJob : TOnRebornJob read FOnRebornJob write FOnRebornJob;
  end;

  { TSortedCustomThread }

  TSortedCustomThread = class(TThread)
  private
    FOwner: TSortedThreadPool;
    FRunning: TThreadBoolean;
    FJob : TLinearJob;
    FThreadKind : TThreadPoolThreadKind;
    FSleepTime : TThreadJobToJobWait;
    FWaitedJobs : TWaitingPool;
    procedure DoOnRebornJob(j : TLinearJob);
    procedure SendForWaiting(j: TLinearJob);
    function GetRunning: Boolean;
    function GetSleepTime: TJobToJobWait;
    function GetThreadKind: TPoolThreadKind;
    procedure SetSleepTime(AValue: TJobToJobWait);
    procedure SetThreadKind(AValue: TPoolThreadKind);
  public
    procedure Execute; override;
    constructor Create(AOwner: TSortedThreadPool;
                               AKind : TPoolThreadKind;
                               aSleepTime : TJobToJobWait);
    destructor Destroy; override;
    property Running: Boolean read GetRunning;
    property myJob : TLinearJob read FJob write FJob;
    property ThreadKind : TPoolThreadKind read GetThreadKind write SetThreadKind;
    property SleepTime : TJobToJobWait read GetSleepTime write SetSleepTime;
  end;

  { TSortedThreadPool }

  TSortedThreadPool = class
  private
    FPool: TThreadSafeFastList;
    FSortedList: TAvgLvlTree;
    FSortedListLocker : TNetCustomLockedObject;
    FLinearList : TThreadSafeFastSeq;
    FSortedThreadsCount : TThreadInteger;
    FLinearThreadsCount : TThreadInteger;
    FThreadJobToJobWait : TThreadJobToJobWait;
    FRunning: TThreadBoolean;
    function GetLinearJobsCount: Integer;
    function GetLinearThreadsCount: integer;
    function GetRunning: Boolean;
    function GetRunningThreads: Integer;
    procedure ExcludeThread(aThread : TSortedCustomThread);
    function GetSortedJob(const TS: QWord): TSortedJob;
    function GetLinearJob(const TS: QWord): TLinearJob;
    function GetSortedJobsCount: Integer;
    function GetSortedThreadsCount: integer;
    function GetThreadJobToJobWait: TJobToJobWait;
    function GetThreadsCount: integer;
    procedure ResizePool({%H-}aSortedThreads: Integer;
                         {%H-}aLinearThreads: Integer);
    procedure SetLinearThreadsCount(AValue: integer);
    procedure SetRunning(AValue: Boolean);
    procedure SetSortedThreadsCount(AValue: integer);
    procedure SetThreadJobToJobWait(AValue: TJobToJobWait);
  public
    constructor Create(const OnCompareMethod: TAvgLvlObjectSortCompare;
                               aSleepTime : TJobToJobWait;
                               aSortedThreads: Integer = 5;
                               aLinearThreads: Integer = 5);
    destructor Destroy; override;
    procedure AddSorted(AJob : TSortedJob);
    procedure AddLinear(AJob : TLinearJob);
    procedure Terminate;
    property Running: Boolean read GetRunning write SetRunning;
    property SortedJobsCount: Integer read GetSortedJobsCount;
    property LinearJobsCount: Integer read GetLinearJobsCount;
    //
    property ThreadsCount : integer read GetThreadsCount;
    property ThreadJobToJobWait : TJobToJobWait read GetThreadJobToJobWait
                                          write SetThreadJobToJobWait;
    property SortedThreadsCount : integer read GetSortedThreadsCount
                                          write SetSortedThreadsCount;
    property LinearThreadsCount : integer read GetLinearThreadsCount
                                          write SetLinearThreadsCount;
  end;

const
  SORTED_DEFAULT_SLEEP_TIME = 5;

const DefaultJobToJobWait : TJobToJobWait = (DefaultValue : SORTED_DEFAULT_SLEEP_TIME;
                                             AdaptMin : 0;
                                             AdaptMax : 0);

implementation

{ TWaitingPool }

procedure TWaitingPool.DoOnRefreshJob(j: TLinearJob; const delta: Cardinal);
var
  target : TWaitingCollection;
begin
  case delta of
   0 : target := nil;
   1..9 : target := F1msPool;
   10..99 : target := F10msPool;
   100..999 : target := F100msPool;
  else
   target := F1000msPool;
  end;

  if assigned(target) then
    target.AddJob(j)
  else
  begin
    if Assigned(FOnRebornJob) then
      FOnRebornJob(j);
  end;
end;

constructor TWaitingPool.Create;
begin
  F1msPool := TWaitingCollection.Create(1);
  F10msPool := TWaitingCollection.Create(10);
  F100msPool := TWaitingCollection.Create(100);
  F1000msPool := TWaitingCollection.Create(1000);

  FTS1ms := GetTickCount64;
  FTS10ms := GetTickCount64;
  FTS100ms := GetTickCount64;
  FTS1000ms := GetTickCount64;
end;

destructor TWaitingPool.Destroy;
begin
  F1msPool.Free;
  F10msPool.Free;
  F100msPool.Free;
  F1000msPool.Free;

  inherited Destroy;
end;

procedure TWaitingPool.AddJob(j: TLinearJob; timeout: Cardinal);
begin
  DoOnRefreshJob(j, timeout);
end;

procedure TWaitingPool.Refresh(const TS: QWord);
var
  Delta : Int32;
begin
  Delta := Int32(Int64(1) + Int64(FTS1ms) - Int64(TS));
  if (Delta <= 0) then
  begin
    F1msPool.Refresh(TS, @DoOnRefreshJob);
    FTS1ms := TS;
  end;
  Delta := Int32(Int64(10) + Int64(FTS10ms) - Int64(TS));
  if (Delta <= 0) then
  begin
    F10msPool.Refresh(TS, @DoOnRefreshJob);
    FTS10ms := TS;
  end;
  Delta := Int32(Int64(100) + Int64(FTS100ms) - Int64(TS));
  if (Delta <= 0) then
  begin
    F100msPool.Refresh(TS, @DoOnRefreshJob);
    FTS100ms := TS;
  end;
  Delta := Int32(Int64(1000) + Int64(FTS1000ms) - Int64(TS));
  if (Delta <= 0) then
  begin
    F1000msPool.Refresh(TS, @DoOnRefreshJob);
    FTS1000ms := TS;
  end;
end;

{ TWaitingCollection }

constructor TWaitingCollection.Create(dt: Cardinal);
begin
  inherited Create;
  FDelta := dt;
end;

procedure TWaitingCollection.AddJob(j: TLinearJob);
var
  i : integer;
begin
  i := IndexOf(nil);
  if i < 0 then
    Add(j)
  else
    Item[i] := j;
end;

procedure TWaitingCollection.DeleteJob(i: integer);
begin
  Item[i] := nil;
end;

procedure TWaitingCollection.Refresh(const TS: QWord; cb : TOnRefreshJob);
var
  i : integer;
  nd : Cardinal;
  j : TLinearJob;
begin
  for i := 0 to Count-1 do
  begin
    j := Item[i];
    if Assigned(j) then
    begin
      nd := Item[i].IsReady(TS);
      if (nd div FDelta) = 0 then
      begin
        Item[i] := nil;
        cb(j, nd);
      end;
    end;
  end;
end;

{ TLinearJob }

function TLinearJob.IsReady(const TS: QWord): Cardinal;
var
  Delta : Int32;
begin
  if FHoldOnValueMs > 0 then
  begin
    Delta := Int32(Int64(FHoldOnValueMs) + Int64(FHoldStart) - Int64(TS));

    if Delta < 0 then
    begin
      Result := 0;
      FHoldOnValueMs := 0;
    end else
      Result := Cardinal(Delta);

  end else Result := 0;
end;

constructor TLinearJob.Create;
begin
  FNeedToRestart := false;
  FHoldOnValueMs := 0;
  FHoldStart:= 0;
end;

procedure TLinearJob.RestartJob(aHoldDelay: Cardinal; const aHoldStart: QWord);
begin
  FNeedToRestart := true;
  FHoldStart := aHoldStart;
  FHoldOnValueMs:=aHoldDelay;
end;

{ TThreadJobToJobWait }

function TThreadJobToJobWait.GetValue: TJobToJobWait;
begin
  Lock;
  try
    Result := FValue;
  finally
    UnLock;
  end;
end;

procedure TThreadJobToJobWait.SetValue(AValue: TJobToJobWait);
begin
  Lock;
  try
    FValue := AValue;
  finally
    UnLock;
  end;
end;

constructor TThreadJobToJobWait.Create(AValue: TJobToJobWait);
begin
  inherited Create;
  if AValue.DefaultValue < 0 then AValue.DefaultValue:= SORTED_DEFAULT_SLEEP_TIME;
  FValue := AValue;
end;

procedure TThreadJobToJobWait.AdaptDropToMin(var AValue: Integer);
begin
  Lock;
  try
    AValue := FValue.AdaptMin;
  finally
    UnLock;
  end;
end;

procedure TThreadJobToJobWait.AdaptToDec(var AValue: Integer);
begin
  Lock;
  try
    AValue := AValue shr 1;
    if AValue < FValue.AdaptMin then
       AValue := FValue.AdaptMin;
  finally
    UnLock;
  end;
end;

procedure TThreadJobToJobWait.AdaptToInc(var AValue: Integer);
begin
  Lock;
  try
    if AValue <= 0 then AValue := 1 else
                        AValue := AValue shl 1;
    if AValue > FValue.AdaptMax then
       AValue := FValue.AdaptMax;
  finally
    UnLock;
  end;
end;

function TThreadJobToJobWait.IsEqual(AValue: TJobToJobWait): Boolean;
begin
  Lock;
  try
    Result := (FValue.DefaultValue = AValue.DefaultValue) and
              (FValue.AdaptMax = AValue.AdaptMax) and
              (FValue.AdaptMin = AValue.AdaptMin);
  finally
    UnLock;
  end;
end;

{ TThreadPoolThreadKind }

function TThreadPoolThreadKind.GetValue: TPoolThreadKind;
begin
  Lock;
  try
    Result := FValue;
  finally
    UnLock;
  end;
end;

procedure TThreadPoolThreadKind.SetValue(AValue: TPoolThreadKind);
begin
  Lock;
  try
    FValue:= AValue;
  finally
    UnLock;
  end;
end;

constructor TThreadPoolThreadKind.Create(AValue: TPoolThreadKind);
begin
  inherited Create;
  FValue:= AValue;
end;

{ TSortedJob }

constructor TSortedJob.Create(aScore: Cardinal);
begin
  FScore:= aScore;
end;

procedure TSortedJob.UpdateScore;
begin
  Inc(FScore);
end;

{ TSortedThreadPool }

function TSortedThreadPool.GetSortedJob(const TS : QWord) : TSortedJob;
var JN : TAvgLvlTreeNode;
begin
  FSortedListLocker.Lock;
  try
    if FSortedList.Count > 0 then
    begin
      JN := FSortedList.FindLowest;
      Result := TSortedJob(JN.Data);
      while assigned(Result) and (Result.IsReady(TS) > 0) do
      begin
        JN := JN.Successor;
        if assigned(JN) then
          Result := TSortedJob(JN.Data) else
          Result := nil;
      end;
      if assigned(JN) then
        FSortedList.Delete(JN);
    end else Result := nil;
  finally
    FSortedListLocker.UnLock;
  end;
end;

function TSortedThreadPool.GetLinearJob(const TS : QWord) : TLinearJob;
begin
  Result := TLinearJob(FLinearList.PopValue);
  if assigned(Result) and (Result.IsReady(TS) > 0) then
  begin
    FLinearList.Push_back(Result);
    Result := nil;
  end;
end;

function TSortedThreadPool.GetSortedJobsCount: Integer;
begin
  FSortedListLocker.Lock;
  try
    Result := FSortedList.Count;
  finally
    FSortedListLocker.UnLock;
  end;
end;

function TSortedThreadPool.GetSortedThreadsCount: integer;
begin
  Result := FSortedThreadsCount.Value;
end;

function TSortedThreadPool.GetThreadJobToJobWait: TJobToJobWait;
begin
  Result := FThreadJobToJobWait.Value;
end;

function TSortedThreadPool.GetThreadsCount: integer;
begin
  Result := SortedThreadsCount + LinearThreadsCount;
end;

procedure TSortedThreadPool.ResizePool(aSortedThreads: Integer;
                                       aLinearThreads: Integer);
var MSC, MLC, i, j : Integer;
    AThread : TSortedCustomThread;
    FT : TJobToJobWait;
begin
  MSC := FSortedThreadsCount.Value;
  MLC := FLinearThreadsCount.Value;
  FPool.Lock;
  try
    for i := 0 to FPool.Count-1 do
    begin
      if assigned(FPool[i]) then
      begin
        AThread := TSortedCustomThread(FPool[i]);
        if AThread.ThreadKind = ptkLinear then
        begin
          if MLC > aLinearThreads then begin
            //need to remove this linear thread
            //or requalificate it to sorted
            if (MSC < aSortedThreads) then
            begin
              Inc(MSC);
              AThread.ThreadKind := ptkSorted;
            end else
              AThread.Terminate;
            Dec(MLC);
          end;
        end else
        begin
          if MSC > aSortedThreads then begin
            //need to remove this sorted thread
            //or requalificate it to linear
            if (MSC < aSortedThreads) then
            begin
              Inc(MLC);
              AThread.ThreadKind := ptkLinear;
            end else
              AThread.Terminate;
            Dec(MSC);
          end;
        end;
      end;
    end;
    FT := ThreadJobToJobWait;
    for i := MLC to aLinearThreads-1 do
    begin
      AThread := TSortedCustomThread.Create(Self, ptkLinear, FT);
      j := FPool.IndexOf(nil);
      if j >= 0 then
        FPool[j] := AThread else
        FPool.Add(AThread);
    end;
    for i := MSC to aSortedThreads-1 do
    begin
      AThread := TSortedCustomThread.Create(Self, ptkSorted, FT);
      j := FPool.IndexOf(nil);
      if j >= 0 then
        FPool[j] := AThread else
        FPool.Add(AThread);
    end;
    FSortedThreadsCount.Value := aSortedThreads;
    FLinearThreadsCount.Value := aLinearThreads;
  finally
    FPool.UnLock;
  end;
end;

procedure TSortedThreadPool.SetLinearThreadsCount(AValue: integer);
begin
  ResizePool(FSortedThreadsCount.Value, AValue);
end;

procedure TSortedThreadPool.SetRunning(AValue: Boolean);
begin
  FRunning.Value:= AValue;
end;

procedure TSortedThreadPool.SetSortedThreadsCount(AValue: integer);
begin
  ResizePool(AValue, FLinearThreadsCount.Value);
end;

procedure TSortedThreadPool.SetThreadJobToJobWait(AValue: TJobToJobWait);
var i : integer;
begin
  if AValue.DefaultValue < 0 then AValue.DefaultValue := SORTED_DEFAULT_SLEEP_TIME;
  if not FThreadJobToJobWait.IsEqual(AValue) then
  begin
    FThreadJobToJobWait.Value := AValue;

    FPool.Lock;
    try
      for i := 0 to FPool.Count-1 do
        if assigned(FPool[i]) then
          TSortedCustomThread(FPool[i]).SleepTime := AValue;
    finally
      FPool.UnLock;
    end;
  end;
end;

function TSortedThreadPool.GetLinearJobsCount: Integer;
begin
  Result := FLinearList.Count;
end;

function TSortedThreadPool.GetLinearThreadsCount: integer;
begin
  Result := FLinearThreadsCount.Value;
end;

function TSortedThreadPool.GetRunning: Boolean;
begin
  Result := FRunning.Value;
end;

function TSortedThreadPool.GetRunningThreads: Integer;
var
  i: Integer;
begin
  FPool.Lock;
  try
    Result := 0;
    for i := 0 to FPool.Count - 1 do
    if assigned(FPool[i]) then
      if TSortedCustomThread(FPool[i]).Running then
        Inc(Result);
  finally
    FPool.UnLock;
  end;
end;

procedure TSortedThreadPool.ExcludeThread(aThread: TSortedCustomThread);
var i : integer;
begin
  FPool.Lock;
  try
    for i := 0 to FPool.Count - 1 do
    begin
      if (FPool[i] = aThread) then begin
        if aThread.ThreadKind = ptkLinear then FLinearThreadsCount.DecValue else
        if aThread.ThreadKind = ptkSorted then FSortedThreadsCount.DecValue;
        FPool[i] := nil;
      end;
    end;
  finally
    FPool.UnLock;
  end;
end;

constructor TSortedThreadPool.Create(
  const OnCompareMethod: TAvgLvlObjectSortCompare;
  aSleepTime: TJobToJobWait;
  aSortedThreads: Integer;
  aLinearThreads: Integer);
var
  i, FThreads: Integer;
  FT : TJobToJobWait;
begin
  FPool:= TThreadSafeFastList.Create;
  FSortedList := TAvgLvlTree.CreateObjectCompare(OnCompareMethod);
  FSortedList.OwnsObjects:=false;
  FSortedListLocker := TNetCustomLockedObject.Create;
  FLinearList := TThreadSafeFastSeq.Create;
  FSortedThreadsCount := TThreadInteger.Create(aSortedThreads);
  FLinearThreadsCount := TThreadInteger.Create(aLinearThreads);
  FThreadJobToJobWait := TThreadJobToJobWait.Create(DefaultJobToJobWait);
  ThreadJobToJobWait := aSleepTime;
  FT := ThreadJobToJobWait;
  FRunning:= TThreadBoolean.Create(false);
  FThreads := aSortedThreads + aLinearThreads;

  FPool.Lock;
  try
    for i := 1 to FThreads do begin
      if i <= aLinearThreads then
       FPool.Add(TSortedCustomThread.Create(Self, ptkLinear, FT)) else
       FPool.Add(TSortedCustomThread.Create(Self, ptkSorted, FT));
      Sleep(FT.DefaultValue div FThreads);
    end;
  finally
    FPool.UnLock;
  end;
end;

destructor TSortedThreadPool.Destroy;
var
  i, cnt: Integer;
  c: TSortedCustomThread;
begin
  FRunning.Value := False;
  cnt := 0;

  FPool.Lock;
  try
    for i := 0 to FPool.Count - 1 do
    begin
      c := TSortedCustomThread(FPool[i]);
      if Assigned(c) then
      begin
        c.Terminate;
        inc(cnt);
      end;
    end;
  finally
    FPool.Unlock;
  end;

  while cnt > 0 do
  begin
    cnt := 0;
    for i := 0 to FPool.Count - 1 do
    begin
      FPool.Lock;
      try
        c := TSortedCustomThread(FPool[i]);
      finally
        FPool.UnLock;
      end;
      if Assigned(c) then
      begin
        inc(cnt);
      end;
    end;
    Sleep(1);
  end;

  FPool.Free;

  FSortedList.OwnsObjects:=true;
  FSortedList.Free;

  FLinearList.Free;

  FSortedListLocker.Free;
  FRunning.Free;
  FLinearThreadsCount.Free;
  FSortedThreadsCount.Free;
  FThreadJobToJobWait.Free;

  inherited Destroy;
end;

procedure TSortedThreadPool.AddSorted(AJob: TSortedJob);
begin
  FSortedListLocker.Lock;
  try
    FSortedList.Add(AJob);
  finally
    FSortedListLocker.UnLock;
  end;
end;

procedure TSortedThreadPool.AddLinear(AJob: TLinearJob);
begin
  FLinearList.Push_back(AJob);
end;

procedure TSortedThreadPool.Terminate;
var
  i: Integer;
begin
  for i := 0 to FPool.Count - 1 do
  if assigned(FPool[i]) then
    TSortedCustomThread(FPool[i]).Terminate;
end;

{ TSortedCustomThread }

procedure TSortedCustomThread.DoOnRebornJob(j: TLinearJob);
begin
  j.NeedToRestart := false;
  if j is TSortedJob then
  begin
    TSortedJob(j).UpdateScore;
    FOwner.AddSorted(TSortedJob(j));
  end else
    FOwner.AddLinear(j);
end;

procedure TSortedCustomThread.SendForWaiting(j: TLinearJob);
begin
  FWaitedJobs.AddJob(j, j.HoldOnValueMs);
end;

function TSortedCustomThread.GetRunning: Boolean;
begin
  if not assigned(FRunning) then Result := false else
  Result := FRunning.Value;
end;

function TSortedCustomThread.GetSleepTime: TJobToJobWait;
begin
  Result := FSleepTime.Value;
end;

function TSortedCustomThread.GetThreadKind: TPoolThreadKind;
begin
  Result := FThreadKind.Value;
end;

procedure TSortedCustomThread.SetSleepTime(AValue: TJobToJobWait);
begin
  FSleepTime.Value := AValue;
end;

procedure TSortedCustomThread.SetThreadKind(AValue: TPoolThreadKind);
begin
  FThreadKind.Value := AValue;
end;

procedure TSortedCustomThread.Execute;
var
  j: TLinearJob;
  TS : QWord;
  s, k : boolean;
  FCurSleepTime : Integer;
begin
  FCurSleepTime := FSleepTime.Value.DefaultValue;
  s := false;
  while not Terminated do
  begin
    if FOwner.Running then
    begin
      TS := GetTickCount64;

      j := nil;
      for k := false to true do
      if not Assigned(j) then
      begin
        if (s = k) then
        begin
          FWaitedJobs.Refresh(TS);
        end
        else
        begin
          case ThreadKind of
            ptkLinear:  j := FOwner.GetLinearJob(TS);
            ptkSorted:  j := FOwner.GetSortedJob(TS);
          else
            j := nil;
          end;
        end;
      end;
      s := not s;
      if Assigned(j) then
      begin
        FRunning.Value := True;
        try
          myJob := j;
          try
            j.Execute;
          except
            //Raise;
          end;
          if j.NeedToRestart then
          begin
            SendForWaiting(j);
            j := nil;
          end;
        finally
          FRunning.Value := False;
          myJob := nil;
          if assigned(j) then j.Free;
        end;
        FSleepTime.AdaptDropToMin(FCurSleepTime);
      end else
        FSleepTime.AdaptToInc(FCurSleepTime);
    end;
    Sleep(FCurSleepTime);
  end;
end;

constructor TSortedCustomThread.Create(AOwner: TSortedThreadPool;
  AKind: TPoolThreadKind; aSleepTime: TJobToJobWait);
begin
  FWaitedJobs := TWaitingPool.Create;
  FWaitedJobs.OnRebornJob := @DoOnRebornJob;
  FOwner := AOwner;
  FRunning := TThreadBoolean.Create(False);
  FSleepTime:= TThreadJobToJobWait.Create(aSleepTime);
  FThreadKind := TThreadPoolThreadKind.Create(AKind);
  inherited Create(False);
  FreeOnTerminate := true;
end;

destructor TSortedCustomThread.Destroy;
begin
  FOwner.ExcludeThread(Self);
  FreeAndNil(FRunning);
  FreeAndNil(FThreadKind);
  FreeAndNil(FSleepTime);
  FreeAndNil(FWaitedJobs);
  inherited Destroy;
end;

end.


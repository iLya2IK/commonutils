{
 SortedThreadPool
   Module for async execution of clients jobs with ranking
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
    FLocTS : QWord;
    FLiveCnt : TAtomicInteger;
    FLocker  : TThreadSafeObject;
    function GetLiveCnt: Integer;
  public
    constructor Create(dt : Cardinal);
    destructor Destroy; override;
    procedure AddJob(j : TLinearJob);
    procedure Refresh(TS : QWord; c : TOnRefreshJob);

    property LiveCnt : Integer read GetLiveCnt;
  end;

  { TWaitingPool }

  TWaitingPool = class(TThread)
  private
    FPools : Array of TWaitingCollection;
    FOnReborn : TOnRebornJob;
    FEvent : PRTLEvent;

    procedure DoOnRefreshJob(j: TLinearJob; const delta: Cardinal);
    function GetLiveCnt: Integer;
  protected
    procedure TerminatedSet; override;
  public
    constructor Create;
    destructor Destroy; override;

    procedure Execute; override;

    procedure AddJob(j : TLinearJob; timeout : Cardinal);

    property OnReborn : TOnRebornJob read FOnReborn write FOnReborn;
    property LiveCnt : Integer read GetLiveCnt;
  end;

  { TSortedCustomThread }

  TSortedCustomThread = class(TThread)
  private
    FOwner: TSortedThreadPool;
    FRunning: TAtomicBoolean;
    FJob : TLinearJob;
    FThreadKind : TThreadPoolThreadKind;
    FSleepTime : TThreadJobToJobWait;
    procedure SendForWaiting(j: TLinearJob);
    function GetRunning: Boolean;
    function GetSleepTime: TJobToJobWait;
    function GetThreadKind: TPoolThreadKind;
    procedure SetSleepTime(AValue: TJobToJobWait);
    procedure SetThreadKind(AValue: TPoolThreadKind);
    function WaitJob : Boolean;
    procedure SignalJob;
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

  { TLinearList }

  TLinearList = class
  private
    FPools : Array [0..7] of TThreadSafeFastSeq;
    FWMarkerLoc, FRMarkerLoc : TAtomicInteger;
    FTotalCount : TAtomicInteger;

    function GetCount : Integer;
  public
    constructor Create;
    destructor Destroy; override;

    procedure Push(const aJob : TLinearJob);
    function PopValue : TLinearJob;

    property Count : Integer read GetCount;
  end;

  { TSortedThreadPool }

  TSortedThreadPool = class
  private
    FPool: TThreadSafeFastList;
    FSortedList: TAvgLvlTree;
    FSortedListLocker : TNetCustomLockedObject;
    FLinearList : TLinearList;
    FSortedThreadsCount : TAtomicInteger;
    FLinearThreadsCount : TAtomicInteger;
    FThreadJobToJobWait : TThreadJobToJobWait;
    FWaitedJobs : TWaitingPool;
    FSortedEvent : PRTLEvent;
    FLinearEvent : PRTLEvent;
    FRunning: TAtomicBoolean;
    procedure DoOnRebornJob(j : TLinearJob);
    procedure LinearJobSignal;
    procedure SortedJobSignal;
    function WaitLinearJobs : Boolean;
    function WaitSortedJobs : Boolean;
    function GetLinearJobsCount: Integer;
    function GetLinearThreadsCount: integer;
    function GetRunning: Boolean;
    function GetRunningThreads: Integer;
    procedure ExcludeThread(aThread : TSortedCustomThread);
    function GetSortedJob: TSortedJob;
    function GetLinearJob: TLinearJob;
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
    procedure AddWaitedJob(AJob : TLinearJob; aDelta : Cardinal);
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

{ TLinearList }

function TLinearList.GetCount: Integer;
begin
  Result := FTotalCount.Value;
end;

constructor TLinearList.Create;
var
  i : integer;
begin
  for i := low(FPools) to high(FPools) do
  begin
    FPools[i] := TThreadSafeFastSeq.Create;
  end;
  FWMarkerLoc := TAtomicInteger.Create(low(FPools));
  FRMarkerLoc := TAtomicInteger.Create(low(FPools));
  FTotalCount := TAtomicInteger.Create(0);
end;

destructor TLinearList.Destroy;
var
  i : integer;
begin
  for i := low(FPools) to high(FPools) do
  begin
    FPools[i].Free;
  end;
  FWMarkerLoc.Free;
  FRMarkerLoc.Free;
  FTotalCount.Free;
  inherited Destroy;
end;

procedure TLinearList.Push(const aJob: TLinearJob);
var loc : Cardinal;
begin
  loc := FWMarkerLoc.Value;
  inc(loc);
  if loc > high(FPools) then loc := low(FPools);
  FWMarkerLoc.Value:= loc;

  FPools[loc].push_back(aJob);
  FTotalCount.IncValue;
end;

function TLinearList.PopValue: TLinearJob;
var loc, i, cur : Cardinal;
begin
  cur := FRMarkerLoc.Value;
  loc := cur;
  for i := low(FPools) to high(FPools) do
  begin
    if loc > high(FPools) then loc := low(FPools);

    Result := TLinearJob(FPools[loc].PopValue);
    if Assigned(Result) then
    begin
      FRMarkerLoc.Value := loc + 1;
      FTotalCount.DecValue;
      Exit;
    end;
    Inc(loc);
  end;
  Result := nil;
end;

{ TWaitingCollection }

function TWaitingCollection.GetLiveCnt: Integer;
begin
  Result := FLiveCnt.Value;
end;

constructor TWaitingCollection.Create(dt: Cardinal);
begin
  inherited Create;
  FDelta := dt;
  FLocTS:= GetTickCount64;
  FLiveCnt := TAtomicInteger.Create(0);
  FLocker:= TThreadSafeObject.Create;
end;

destructor TWaitingCollection.Destroy;
begin
  FLiveCnt.Free;
  FLocker.Free;
  inherited Destroy;
end;

procedure TWaitingCollection.AddJob(j: TLinearJob);
var
  i : integer;
begin
  FLocker.Lock;
  try
    i := IndexOf(nil);
    if i < 0 then
      Add(j)
    else
      Item[i] := j;
  finally
    FLocker.UnLock;
  end;

  FLiveCnt.IncValue;
end;

procedure TWaitingCollection.Refresh(TS: QWord; c: TOnRefreshJob);
var
  cnt, i : integer;
  nd : Cardinal;
  j : TLinearJob;
begin
  if Int32(Int64(TS) - Int64(FLocTS)) >= Int32(FDelta) then
  begin
    FLocTS:= TS;
    FLocker.Lock;
    try
      cnt := Count;
    finally
      FLocker.UnLock;
    end;
    for i := 0 to cnt-1 do
    begin
      FLocker.Lock;
      try
        j := Item[i];
      finally
        FLocker.UnLock;
      end;
      if Assigned(j) then
      begin
        nd := j.IsReady(TS);
        if (nd div FDelta) = 0 then
        begin
          FLocker.Lock;
          try
            Item[i] := nil;
          finally
            FLocker.UnLock;
          end;
          FLiveCnt.DecValue;
          c(j, nd);
        end;
      end;
    end;
  end;
end;

{ TWaitingPool }

procedure TWaitingPool.DoOnRefreshJob(j: TLinearJob; const delta: Cardinal);
var
  target : TWaitingCollection;
begin
  case (delta div 4) of
   0 : target := nil;
   1..9 : target := FPools[0];
   10..99 : target := FPools[1];
   100..999 : target := FPools[2];
  else
   target := FPools[3];
  end;

  if assigned(target) then
  begin
    target.AddJob(j);
    RTLEventSetEvent(FEvent);
  end
  else
  begin
    if Assigned(FOnReborn) then
      FOnReborn(j) else
      j.Free;
  end;
end;

function TWaitingPool.GetLiveCnt: Integer;
var i : integer;
begin
  Result := 0;
  for i := 0 to high(FPools) do
  begin
    Result := Result + FPools[i].LiveCnt;
  end;
end;

procedure TWaitingPool.TerminatedSet;
begin
  inherited TerminatedSet;
  RTLEventSetEvent(FEvent);
end;

constructor TWaitingPool.Create;
var i, c : integer;
begin
  inherited Create(true);

  FEvent:= RTLEventCreate;
  SetLength(FPools, 4);
  c := 1;
  for i := 0 to High(FPools) do
  begin
    FPools[i] := TWaitingCollection.Create(c);
    c := c * 10;
  end;
end;

destructor TWaitingPool.Destroy;
var
  i : integer;
begin
  for i := 0 to High(FPools) do
    FPools[i].Free;

  RTLEventDestroy(FEvent);

  inherited Destroy;
end;

procedure TWaitingPool.Execute;
var
  i : integer;
  TS : QWord;
begin
  TS := GetTickCount64;

  while not Terminated do
  begin
    RTLEventWaitFor(FEvent);

    TS := GetTickCount64;
    for i := 0 to High(FPools) do
    begin
      FPools[i].Refresh(TS, @DoOnRefreshJob)
    end;

    if LiveCnt > 0 then
      RTLEventSetEvent(FEvent);

    Sleep(1);
  end;
end;

procedure TWaitingPool.AddJob(j: TLinearJob; timeout: Cardinal);
begin
  DoOnRefreshJob(j, timeout);
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

function TSortedThreadPool.GetSortedJob : TSortedJob;
var JN : TAvgLvlTreeNode;
begin
  FSortedListLocker.Lock;
  try
    if FSortedList.Count > 0 then
    begin
      JN := FSortedList.FindLowest;
      Result := TSortedJob(JN.Data);
      if assigned(JN) then
        FSortedList.Delete(JN);
    end else Result := nil;
  finally
    FSortedListLocker.UnLock;
  end;
end;

function TSortedThreadPool.GetLinearJob : TLinearJob;
begin
  Result := TLinearJob(FLinearList.PopValue);
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

procedure TSortedThreadPool.AddWaitedJob(AJob: TLinearJob; aDelta: Cardinal);
begin
  FWaitedJobs.DoOnRefreshJob(AJob, aDelta);
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
        case aThread.ThreadKind of
          ptkLinear: FLinearThreadsCount.DecValue;
          ptkSorted: FSortedThreadsCount.DecValue;
        end;
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
  FSortedEvent := RTLEventCreate;
  FLinearEvent := RTLEventCreate;
  FPool:= TThreadSafeFastList.Create;
  FWaitedJobs := TWaitingPool.Create;
  FWaitedJobs.OnReborn := @DoOnRebornJob;
  FSortedList := TAvgLvlTree.CreateObjectCompare(OnCompareMethod);
  FSortedList.OwnsObjects:=false;
  FSortedListLocker := TNetCustomLockedObject.Create;
  FLinearList := TLinearList.Create;
  FSortedThreadsCount := TAtomicInteger.Create(aSortedThreads);
  FLinearThreadsCount := TAtomicInteger.Create(aLinearThreads);
  FThreadJobToJobWait := TThreadJobToJobWait.Create(DefaultJobToJobWait);
  ThreadJobToJobWait := aSleepTime;
  FT := ThreadJobToJobWait;
  FRunning:= TAtomicBoolean.Create(false);
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

  FWaitedJobs.Start;
end;

destructor TSortedThreadPool.Destroy;
var
  i, cnt: Integer;
  c: TSortedCustomThread;
begin
  FRunning.Value := False;
  cnt := 0;

  FWaitedJobs.Terminate;
  FWaitedJobs.WaitFor;
  FWaitedJobs.Free;

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
    LinearJobSignal;
    SortedJobSignal;
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

  RTLEventDestroy(FLinearEvent);
  RTLEventDestroy(FSortedEvent);
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
  SortedJobSignal;
end;

procedure TSortedThreadPool.AddLinear(AJob: TLinearJob);
begin
  FLinearList.Push(AJob);
  LinearJobSignal;
end;

procedure TSortedThreadPool.Terminate;
var
  i: Integer;
begin
  for i := 0 to FPool.Count - 1 do
  if assigned(FPool[i]) then
    TSortedCustomThread(FPool[i]).Terminate;

  SortedJobSignal;
  LinearJobSignal;
end;

procedure TSortedThreadPool.DoOnRebornJob(j: TLinearJob);
begin
  j.NeedToRestart := false;
  if j is TSortedJob then
  begin
    TSortedJob(j).UpdateScore;
    AddSorted(TSortedJob(j));
  end else
    AddLinear(j);
end;

procedure TSortedThreadPool.LinearJobSignal;
begin
  RTLEventSetEvent(FLinearEvent);
end;

procedure TSortedThreadPool.SortedJobSignal;
begin
  RTLEventSetEvent(FSortedEvent);
end;

function TSortedThreadPool.WaitLinearJobs: Boolean;
begin
  RTLEventWaitFor(FLinearEvent);
  Result := (FLinearList.Count = 0);
end;

function TSortedThreadPool.WaitSortedJobs: Boolean;
begin
  RTLEventWaitFor(FSortedEvent);
  Result := (FSortedList.Count = 0);
end;

{ TSortedCustomThread }

procedure TSortedCustomThread.SendForWaiting(j: TLinearJob);
begin
  FOwner.AddWaitedJob(j, j.HoldOnValueMs);
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

function TSortedCustomThread.WaitJob : Boolean;
begin
  case ThreadKind of
    ptkLinear:  Result := FOwner.WaitLinearJobs;
    ptkSorted:  Result := FOwner.WaitSortedJobs;
  else
    Result := false;
  end;
end;

procedure TSortedCustomThread.SignalJob;
begin
  case ThreadKind of
    ptkLinear: FOwner.LinearJobSignal;
    ptkSorted: FOwner.SortedJobSignal;
  end;
end;

procedure TSortedCustomThread.Execute;
var
  j: TLinearJob;
  FCurSleepTime : Integer;
begin
  FCurSleepTime := FSleepTime.Value.DefaultValue;
  while (not Terminated) do
  begin
    if FOwner.Running then
    begin
      if WaitJob then
      begin
        Sleep(FCurSleepTime);
        FSleepTime.AdaptToInc(FCurSleepTime);
        Continue;
      end;

      case ThreadKind of
        ptkLinear:  j := FOwner.GetLinearJob;
        ptkSorted:  j := FOwner.GetSortedJob;
      else
        j := nil;
      end;

      if Assigned(j) then
      begin
        SignalJob;

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
      end;
    end else
      Sleep(FCurSleepTime);
  end;
end;

constructor TSortedCustomThread.Create(AOwner: TSortedThreadPool;
  AKind: TPoolThreadKind; aSleepTime: TJobToJobWait);
begin
  FOwner := AOwner;
  FRunning := TAtomicBoolean.Create(False);
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
  inherited Destroy;
end;

end.


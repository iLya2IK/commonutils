{
 ExtSqlite3DS:
   Extension of CustomSqliteDataset module to use sqlite preparation 
   objects and functions. Added faster routes for unidirectional viewing.

   Part of ESolver project

   Copyright (c) 2019-2020 by Ilya Medvedkov

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
}

unit ExtSqlite3DS;

{$mode objfpc}{$H+}

interface

uses
  Classes, SysUtils, StrUtils,
  db,
  OGLFastList,
  BufferedStream,
  variants,
  {$IFDEF LOAD_DYNAMICALLY}
  SQLite3Dyn,
  {$ELSE}
  SQLite3,
  {$ENDIF}
  CustomSQLiteDS;

{$define extra_prepared_thread_safe}
{$ifdef extra_prepared_thread_safe}
   {prepared_multi_holders option increases speed x(1.2-1.5) of multithread
    applications when similar prepared objects are used in parallel.
    To get all benefits of this mode, you need the SQLITE_OPEN_NOMUTEX option
    setted as flag that given as the third argument to sqlite3_open_v2().
    nb: use this if there are only SELECT requests used. otherwise you can
        expect instant lock or speed decreasing. (journal_mode=WAL can help)}
   {.$define prepared_multi_holders}
{$endif}

type
  TSqlite3FuncStyle = (sqlfScalar, sqlfAggregate);
  TSqlite3TextEncode = ( sqlteUtf8,
                         sqlteUtf16LE,
                         sqlteUtf16BE,
                         sqlteUtf16,
                         sqlteAny,
                         sqlteUtf16_ALIGNED );

  { TSqlite3Function }
  TSqlite3Function = class
  private
    FName : String;
    FTextEncode : TSqlite3TextEncode;
    FFuncStyle  : TSqlite3FuncStyle;
    FDeterministic : Boolean;
    FContext : psqlite3_context;
    FArguments : ppsqlite3_value;
    FParCnt : Integer;
    procedure SetContext(context : psqlite3_context);
    procedure InternalScalarFunc(context : psqlite3_context; argc : integer; argv : ppsqlite3_value); virtual;
    procedure InternalStepFunc(context : psqlite3_context; argc : integer; argv : ppsqlite3_value); virtual;
    procedure InternalFinalFunc(context : psqlite3_context); virtual;
    procedure Reconnect(db : psqlite3);
  public
    constructor Create(const aName : String; aParCnt : Integer; aTextEncode : TSqlite3TextEncode;
      aFuncStyle : TSqlite3FuncStyle; isDeterministic : Boolean = true);
    destructor Destroy; override;

    function GetAggregateContext(aSize : integer) : Pointer;

    procedure SetResult(res : Variant);
    function  GetArgument(arg : Integer) : Variant;
    function AsInt(arg : Integer) : Integer;
    function AsInt64(arg : Integer) : Int64;
    function AsDouble(arg : Integer) : Double;
    function AsString(arg : Integer) : String;
    function AsNull({%H-}arg : integer) : Pointer;

    procedure ScalarFunc({%H-}argc : integer); virtual;
    procedure StepFunc({%H-}argc : integer); virtual;
    procedure FinalFunc; virtual;
    procedure Disconnect; virtual;
  end;

  TExtSqlite3Dataset = class;

  TSqlite3Prepared = class;

  {TSqlite3PreparedHolder}
  TSqlite3PreparedHolder = class
  private
    FOwner      : TSqlite3Prepared;
    FColumns    : TStringList;
    vm          : pointer;
    FReturnCode : integer;
    ready       : Boolean;
    {$ifdef extra_prepared_thread_safe}
    FRTI        : TRTLCriticalSection;
    {$endif}
    procedure ConsumeColumns;
    function GetColumn(Index : Integer): String;
    function GetColumnCount: Integer;
    procedure Reconnect(db : psqlite3);
    function BindParametres(const Params : Array of Const) : Boolean;
    function ReturnString : String;
  public
    constructor Create(aOwner : TSqlite3Prepared);
    function QuickQuery(const Params : Array of Const; const AStrList: TStrings; FillObjects:Boolean): String;
    procedure Execute(const Params : Array of Const);
    procedure Disconnect;
    {$ifdef extra_prepared_thread_safe}
    procedure Lock; inline;
    procedure UnLock; inline;
    {$endif}
    function Open(const Params: array of Const): Boolean;
    function Step : Boolean;
    procedure Close;
    destructor Destroy; override;

    property ColumnCount : integer read GetColumnCount;
    property Columns[Index : Integer] : String read GetColumn;
  end;

  {$ifdef prepared_multi_holders}
  TThreadArray = Array of TSqlite3PreparedHolder;
  {$endif}

  { TSqlite3Prepared }
  TSqlite3Prepared = class
  private
    FOwner      : TExtSqlite3Dataset;
    {$ifdef prepared_multi_holders}
    FThreadArray   : TThreadArray;
    FThreadMask    : Byte;
    procedure SetThreadHolderLength(cnt : SmallInt);
    {$else}
    FHolder     : TSqlite3PreparedHolder;
    {$endif}
  private
    FExpr       : String;
    FOnPostExecute : TNotifyEvent;
    function GetThreadHolder : TSqlite3PreparedHolder;{$ifndef prepared_multi_holders}inline;{$endif}
    function GetColumn(Index : Integer): String;
    function GetColumnCount: integer;
    procedure Reconnect(db : psqlite3);
    function  DoBindParametres({%H-}const Params : Array of Const) : Boolean; virtual;
    procedure DoPostExecute;
    procedure InitPrepared(aOwner : TExtSqlite3Dataset; const aSQL : String);
  public
    constructor Create(aOwner : TExtSqlite3Dataset; const aSQL : String); virtual;
    {$ifdef prepared_multi_holders}
    constructor Create(aOwner : TExtSqlite3Dataset; const aSQL : String;
      ThreadsCnt : SmallInt); overload;
    {$endif}
    function QuickQuery(const Params : Array of Const; const AStrList: TStrings; FillObjects:Boolean): String; virtual;
    procedure Execute(const Params : Array of Const); virtual;
    procedure Execute; overload;
    procedure Disconnect; virtual;
    {$ifdef extra_prepared_thread_safe}
    procedure Lock; inline;
    procedure UnLock; inline;
    {$endif}
    function Open(const Params: array of const): Boolean;
    function Step : Boolean;
    procedure Close;
    destructor Destroy; override;

    property ColumnCount : integer read GetColumnCount;
    property Columns[Index : Integer] : String read GetColumn;
    property OnPostExecute : TNotifyEvent read FOnPostExecute write FOnPostExecute;
  end;

  TSqlite3PreparedClass = class of TSqlite3Prepared;

  TExtSqlite3OnPrepared = procedure (const aRequest : String; aResult : Integer) of object;

  TExtSqlite3OpenMode = (eomNormal, eomUniDirectional);

  TExtSqlite3OpenFlag = (sqlite_OpenReadOnly, sqlite_OpenReadWrite,
                         sqlite_OpenCreate, sqlite_OpenDeleteOnClose,
                         sqlite_OpenExclusive, sqlite_OpenAutoPROXY,
                         sqlite_OpenURI, sqlite_OpenMemory,
                         sqlite_OpenMainDB, sqlite_OpenTempDB,
                         sqlite_OpenTransientDB, sqlite_OpenMainJournal,
                         sqlite_OpenTempJournal, sqlite_OpenSubJournal,
                         sqlite_OpenSuperJournal, sqlite_OpenNoMutex,
                         sqlite_OpenFullMutex, sqlite_OpenSharedCache,
                         sqlite_OpenPrivateCache, sqlite_OpenWAL,
                         sqlite_OpenNoFollow);
  TExtSqlite3OpenFlags = set of TExtSqlite3OpenFlag;

  TSqlite3FuncCollection = class(specialize TFastBaseCollection<TSqlite3Function>);
  TSqlite3PrepCollection = class(specialize TFastBaseCollection<TSqlite3Prepared>);

  { TExtSqlite3Dataset }

  TExtSqlite3Dataset = class(TCustomSqliteDataset)
  private
    FExtFlags : TExtSqlite3OpenFlags;
    FFunctions : TSqlite3FuncCollection;
    FPrepared  : TSqlite3PrepCollection;
    FRTI: TRTLCriticalSection;
    FOpenMode  : TExtSqlite3OpenMode;
    FUniDirPrepared : Pointer;
    FDefaultStringSize : Integer;
    FOnPrepared : TExtSqlite3OnPrepared;
    procedure ConsumeRow(aRecord : PPAnsiChar; ColumnCount : integer);
    procedure SetDefaultStringSize(AValue: Integer);
    procedure DoRequestPrepared(const aRequest : String; aResult : Integer);
  protected
    procedure BuildLinkedList; override;
    function GetLastInsertRowId: Int64; override;
    function GetRowsAffected: Integer; override;
    procedure RetrieveFieldDefs; override;
    function SqliteExec(ASQL: PAnsiChar; ACallback: TSqliteCdeclCallback;
      Data: Pointer): Integer; override;
    function GenReturnString(aReturnCode : Cardinal) : String;
    procedure ReconnectFunctions(aSqliteHandle: psqlite3);
    procedure ReconnectPrepared(aSqliteHandle: psqlite3);
    function  InternalGetHandle: Pointer; override;
    procedure InternalCloseHandle; override;
    function  GetRecord(Buffer: TRecordBuffer; GetMode: TGetMode; DoCheck: Boolean): TGetResult; override;
    function  GetNextRecords: Longint; override;
    procedure InternalClose; override;
  public
    constructor Create(AOwner: TComponent); override;
    procedure AddFunction(aFunc : TSqlite3Function);
    procedure AddPrepared(aPrep : TSqlite3Prepared);
    function AddNewPrep(const aSQL: String{$ifdef prepared_multi_holders};
      ThreadCnt : SmallInt = -1 {$endif}): TSqlite3Prepared;
    procedure ClearPrepared;

    procedure ExecuteDirect(const ASQL: String); override;
    function QuickQuery(const ASQL: String; const AStrList: TStrings; FillObjects: Boolean): String; override;

    procedure Open(aMode : TExtSqlite3OpenMode); overload;
    function  GetFieldData(Field: TField; Buffer: Pointer; NativeFormat: Boolean): Boolean; override;
    function  CreateBlobStream(Field: TField; Mode: TBlobStreamMode): TStream; override;

    function ReturnString: String; override;
    procedure Lock; inline;
    procedure UnLock; inline;
    destructor Destroy; override;
    class function SqliteVersion: String; override;
    class function Sqlite3ExtFlagsToFFOO(flags : TExtSqlite3OpenFlags
      ) : Cardinal;

    property ExtOpenMode : TExtSqlite3OpenMode read FOpenMode;
    property ExtFlags : TExtSqlite3OpenFlags read FExtFlags write FExtFlags;
    property DefaultStringSize : Integer read FDefaultStringSize write SetDefaultStringSize;
    property OnPrepared : TExtSqlite3OnPrepared read FOnPrepared write FOnPrepared;
  end;

  function SqliteCode2Str(Code: Integer): String;

implementation

{$ifdef prepared_multi_holders}
const DEFAULT_THREAD_HOLDER_CNT = 1;
{$endif}

function SqliteCode2Str(Code: Integer): String;
begin
  case Code of
    SQLITE_OK           : Result := 'SQLITE_OK';
    SQLITE_ERROR        : Result := 'SQLITE_ERROR';
    SQLITE_INTERNAL     : Result := 'SQLITE_INTERNAL';
    SQLITE_PERM         : Result := 'SQLITE_PERM';
    SQLITE_ABORT        : Result := 'SQLITE_ABORT';
    SQLITE_BUSY         : Result := 'SQLITE_BUSY';
    SQLITE_LOCKED       : Result := 'SQLITE_LOCKED';
    SQLITE_NOMEM        : Result := 'SQLITE_NOMEM';
    SQLITE_READONLY     : Result := 'SQLITE_READONLY';
    SQLITE_INTERRUPT    : Result := 'SQLITE_INTERRUPT';
    SQLITE_IOERR        : Result := 'SQLITE_IOERR';
    SQLITE_CORRUPT      : Result := 'SQLITE_CORRUPT';
    SQLITE_NOTFOUND     : Result := 'SQLITE_NOTFOUND';
    SQLITE_FULL         : Result := 'SQLITE_FULL';
    SQLITE_CANTOPEN     : Result := 'SQLITE_CANTOPEN';
    SQLITE_PROTOCOL     : Result := 'SQLITE_PROTOCOL';
    SQLITE_EMPTY        : Result := 'SQLITE_EMPTY';
    SQLITE_SCHEMA       : Result := 'SQLITE_SCHEMA';
    SQLITE_TOOBIG       : Result := 'SQLITE_TOOBIG';
    SQLITE_CONSTRAINT   : Result := 'SQLITE_CONSTRAINT';
    SQLITE_MISMATCH     : Result := 'SQLITE_MISMATCH';
    SQLITE_MISUSE       : Result := 'SQLITE_MISUSE';
    SQLITE_NOLFS        : Result := 'SQLITE_NOLFS';
    SQLITE_AUTH         : Result := 'SQLITE_AUTH';
    SQLITE_FORMAT       : Result := 'SQLITE_FORMAT';
    SQLITE_RANGE        : Result := 'SQLITE_RANGE';
    SQLITE_ROW          : Result := 'SQLITE_ROW';
    SQLITE_NOTADB       : Result := 'SQLITE_NOTADB';
    SQLITE_DONE         : Result := 'SQLITE_DONE';
  else
    Result := 'Unknown Return Value';
  end;
end;

function GetAutoIncValue(NextValue: Pointer; {%H-}Columns: Integer;
  ColumnValues: PPAnsiChar; {%H-}ColumnNames: PPAnsiChar): Integer; cdecl;
var
  CodeError, TempInt: Integer;
begin
  TempInt := 0;
  if ColumnValues[0] <> nil then
  begin
    Val(String(ColumnValues[0]), TempInt, CodeError);
    if CodeError <> 0 then
      DatabaseError('TSqlite3Dataset: Error trying to get last autoinc value');
  end;
  Integer(NextValue^) := Succ(TempInt);
  Result := 1;
end;

function GetSqlite3TextEncode(aTE : TSqlite3TextEncode) : Cardinal;
begin
  case aTE of
    sqlteUtf8 : Result := SQLITE_UTF8;
    sqlteUtf16LE : Result := SQLITE_UTF16LE;
    sqlteUtf16BE : Result := SQLITE_UTF16BE;
    sqlteUtf16 : Result := SQLITE_UTF16;
    sqlteAny : Result := SQLITE_ANY;
    sqlteUtf16_ALIGNED : Result := SQLITE_UTF16_ALIGNED;
  end;
end;

procedure scalar_func(context : psqlite3_context; argc : integer; argv : ppsqlite3_value); cdecl;
var data : Pointer;
begin
  data := sqlite3_user_data(context);
  TSqlite3Function(data).InternalScalarFunc(context, argc, argv);
end;

procedure step_func(context : psqlite3_context; argc : integer; argv : ppsqlite3_value); cdecl;
var data : Pointer;
begin
  data := sqlite3_user_data(context);
  TSqlite3Function(data).InternalStepFunc(context, argc, argv);
end;

procedure final_func(context : psqlite3_context); cdecl;
var data : Pointer;
begin
  data := sqlite3_user_data(context);
  TSqlite3Function(data).InternalFinalFunc(context);
end;

procedure destroy_func(data : pointer); cdecl;
begin
  TSqlite3Function(data).Disconnect();
end;

procedure freebindstring(astring: pointer); cdecl;
begin
  StrDispose(AString);
end;

Function PCharStr(Const S : String) : PChar;
begin
  Result:=StrAlloc(Length(S)+1);
  If (Result<>Nil) then
    StrPCopy(Result,S);
end;

{ TSqlite3Prepared }

function TSqlite3Prepared.GetThreadHolder : TSqlite3PreparedHolder;
{$ifdef prepared_multi_holders}
var TID : Byte;
begin
  {$ifdef linux}
  TID := Byte(GetThreadID shr 12) and FThreadMask;
  {$else}
  TID := Byte(GetThreadID) and FThreadMask;
  {$endif}
  Result := FThreadArray[TID];
{$else}
begin
  Result := FHolder;
{$endif}
end;

{$ifdef prepared_multi_holders}
procedure TSqlite3Prepared.SetThreadHolderLength(cnt : SmallInt);
var i : integer;
begin
  if cnt <= 0 then cnt := DEFAULT_THREAD_HOLDER_CNT;
  i := cnt shr 1;
  FThreadMask := 0;
  while i > 0 do
  begin
    FThreadMask := FThreadMask shl 1 or $01;
    i := i shr 1;
  end;
  cnt := FThreadMask + 1;
  SetLength(FThreadArray, cnt);
  for i := 0 to cnt-1 do FThreadArray[i] := nil;
end;
{$endif}

{$ifdef EXTRA_PREPARED_THREAD_SAFE}
procedure TSqlite3Prepared.Lock;
begin
  GetThreadHolder.Lock;
end;

procedure TSqlite3Prepared.UnLock;
begin
  GetThreadHolder.UnLock;
end;
{$endif}

function TSqlite3Prepared.GetColumn(Index : Integer) : String;
begin
  Result := GetThreadHolder.Columns[Index];
end;

function TSqlite3Prepared.GetColumnCount : integer;
begin
  Result := GetThreadHolder.ColumnCount;
end;

procedure TSqlite3Prepared.Reconnect(db : psqlite3);
{$ifdef prepared_multi_holders}
var it : integer;
begin
  for it := 0 to High(FThreadArray) do
  begin
    if not Assigned(FThreadArray[it]) then
      FThreadArray[it] := TSqlite3PreparedHolder.Create(Self);
    FThreadArray[it].Reconnect(db);
  end;
  {$else}
begin
  FHolder.Reconnect(db);
  {$endif}
end;

function TSqlite3Prepared.DoBindParametres(
  {%H-}const Params : array of const) : Boolean;
begin
  // override this
  Result := true;
end;

procedure TSqlite3Prepared.DoPostExecute;
begin
  if Assigned(FOnPostExecute) then FOnPostExecute(Self);
end;

procedure TSqlite3Prepared.InitPrepared(aOwner : TExtSqlite3Dataset;
  const aSQL : String);
begin
  FExpr := aSQL;
  FOwner := aOwner;
  {$ifndef prepared_multi_holders}
  FHolder := TSqlite3PreparedHolder.Create(Self);
  {$endif}
end;

constructor TSqlite3Prepared.Create(aOwner : TExtSqlite3Dataset;
  const aSQL : String);
begin
  InitPrepared(aOwner, aSQL);
  {$ifdef prepared_multi_holders}
  SetThreadHolderLength(-1);
  {$endif}
end;

{$ifdef prepared_multi_holders}
constructor TSqlite3Prepared.Create(aOwner : TExtSqlite3Dataset;
  const aSQL : String; ThreadsCnt : SmallInt);
begin
  InitPrepared(aOwner, aSQL);
  SetThreadHolderLength(ThreadsCnt);
end;
{$endif}

function TSqlite3Prepared.QuickQuery(const Params : array of const;
  const AStrList : TStrings; FillObjects : Boolean) : String;
begin
  {$ifdef extra_prepared_thread_safe}
  Lock;
  try
  {$endif}
  Result := GetThreadHolder.QuickQuery(Params, AStrList, FillObjects);
  {$ifdef extra_prepared_thread_safe}
  finally
  UnLock;
  end;
  {$endif}
end;

procedure TSqlite3Prepared.Execute(const Params : array of const);
begin
  {$ifdef extra_prepared_thread_safe}
  Lock;
  try
  {$endif}
  GetThreadHolder.Execute(Params);
  {$ifdef extra_prepared_thread_safe}
  finally
  UnLock;
  end;
  {$endif}
end;

procedure TSqlite3Prepared.Execute;
begin
  Execute([]);
end;

procedure TSqlite3Prepared.Disconnect;
{$ifdef prepared_multi_holders}
var it : Integer;
begin
  for it := 0 to High(FThreadArray) do
  begin
    if Assigned(FThreadArray[it]) then
      FThreadArray[it].Disconnect;
  end;
  {$else}
begin
  FHolder.Disconnect;
  {$endif}
end;

function TSqlite3Prepared.Open(const Params : array of const) : Boolean;
begin
  Result := GetThreadHolder.Open(Params);
end;

function TSqlite3Prepared.Step : Boolean;
begin
  Result := GetThreadHolder.Step;
end;

procedure TSqlite3Prepared.Close;
begin
  GetThreadHolder.Close;
end;

destructor TSqlite3Prepared.Destroy;
{$ifdef prepared_multi_holders}
var it : Integer;
begin
  for it := 0 to High(FThreadArray) do
  begin
    if Assigned(FThreadArray[it]) then
      FThreadArray[it].Free;
  end;
  {$else}
begin
  FHolder.Free;
  {$endif}
  inherited Destroy;
end;

{ TSqlite3PreparedHolder }

procedure TSqlite3PreparedHolder.Reconnect(db: psqlite3);
begin
  vm := nil;
  ready := false;
  FColumns.Clear;
  if db = nil then exit;
  FReturnCode := sqlite3_prepare_v2(db, PAnsiChar(FOwner.FExpr), -1, @vm, nil);
  if FReturnCode <> SQLITE_OK then
  begin
    DatabaseError(ReturnString, FOwner.FOwner);
  end else
    ready := true;
end;

function TSqlite3PreparedHolder.GetColumn(Index : Integer): String;
begin
  Result := FColumns[index];
end;

function TSqlite3PreparedHolder.GetColumnCount : Integer;
begin
  Result := FColumns.Count;
end;

function TSqlite3PreparedHolder.BindParametres(const Params: array of const): Boolean;
var i, p : integer;
    S : String;
begin
  Result := true;
  I := Low(Params);
  P := I + 1;
  While I<=High(Params) do
    begin
      With Params[i] do
        case VType of
          vtInteger    : FReturnCode := sqlite3_bind_int(vm, p, VInteger);
          vtBoolean    : FReturnCode := sqlite3_bind_int(vm, p, Integer(VBoolean));
          vtChar       : FReturnCode := sqlite3_bind_text(vm, p, pcharstr(VChar), 1, @freebindstring);
          vtExtended   : FReturnCode := sqlite3_bind_double(vm, p, VExtended^);
          vtString     : FReturnCode := sqlite3_bind_text(vm, p, pcharstr(VString^), length(VString^), @freebindstring);
          vtAnsiString : FReturnCode := sqlite3_bind_text(vm, p,
                       pcharstr(UTF8Encode(String(VAnsiString))),
                       length(String(VAnsiString)), @freebindstring);
          vtWideString : FReturnCode := sqlite3_bind_text(vm, p,
                       pcharstr(UTF8Encode(WideCharToString(VWideString))),
                       length(WideCharToString(VWideString)), @freebindstring);
          vtPointer    : If (VPointer=Nil) then
                           FReturnCode := sqlite3_bind_null(vm, p);
          vtInt64      : FReturnCode := sqlite3_bind_int64(vm, p, VInt64^);
          vtQWord      : FReturnCode := sqlite3_bind_int64(vm, p, VQWord^);
          vtVariant    : begin
                       case VarType(VVariant^) of
                         varempty, varnull, varunknown : FReturnCode := sqlite3_bind_null(vm, p);
                         varsmallint, varinteger,
                            varboolean, varshortint,
                            varbyte, varword, varlongword : FReturnCode := sqlite3_bind_int(vm, p, Integer(VVariant^));
                         varstring, varustring, varolestr : begin
                           S := VarToStr(VVariant^);
                           FReturnCode := sqlite3_bind_text(vm, p, pcharstr(S), length(S), @freebindstring);
                         end;
                         varsingle, vardouble : FReturnCode := sqlite3_bind_double(vm, p, Double(VVariant^));
                         varword64, varint64 : FReturnCode := sqlite3_bind_int64(vm, p, Int64(VVariant^));
                       else
                         FReturnCode := sqlite3_bind_null(vm, p);
                       end;
            end;
          //vtObject     :
        end;
      if FReturnCode <> SQLITE_OK then
      begin
        DatabaseError(ReturnString, FOwner.FOwner);
        Exit(false);
      end;
      Inc(I);
      Inc(P);
    end;
  FOwner.DoBindParametres(Params);
end;

procedure TSqlite3PreparedHolder.ConsumeColumns;
var i : integer;
begin
  for i := 0 to ColumnCount-1 do
  begin
    FColumns[i] :=String(sqlite3_column_text(vm,i));
  end;
end;

function TSqlite3PreparedHolder.ReturnString: String;
begin
  Result := FOwner.FOwner.GenReturnString(FReturnCode);
end;

constructor TSqlite3PreparedHolder.Create(aOwner: TSqlite3Prepared);
begin
  FColumns := TStringList.Create;
  FOwner := aOwner;
  ready:= false;
  vm := nil;
  {$ifdef extra_prepared_thread_safe}
  InitCriticalSection(FRTI);
  {$endif}
end;

function TSqlite3PreparedHolder.QuickQuery(const Params: array of const;
  const AStrList: TStrings; FillObjects: Boolean): String;
procedure FillStrings;
begin
  while FReturnCode = SQLITE_ROW do
  begin
    AStrList.Add(String(sqlite3_column_text(vm,0)));
    FReturnCode := sqlite3_step(vm);
  end;
end;
procedure FillStringsAndObjects;
begin
  while FReturnCode = SQLITE_ROW do
  begin
    AStrList.AddObject(String(sqlite3_column_text(vm, 0)),
      TObject(PtrInt(sqlite3_column_int(vm, 1))));
    FReturnCode := sqlite3_step(vm);
  end;
end;
begin
  if not ready then exit('');

  if not BindParametres(Params) then Exit('');

  FReturnCode := sqlite3_step(vm);
  if (FReturnCode = SQLITE_ROW) and (sqlite3_column_count(vm) > 0) then
  begin
    Result := String(sqlite3_column_text(vm, 0));
    if Assigned(AStrList) then
    begin
      if FillObjects and (sqlite3_column_count(vm) > 1) then
        FillStringsAndObjects
      else
        FillStrings;
    end;
  end;
  sqlite3_reset(vm);
  FOwner.DoPostExecute;
end;

procedure TSqlite3PreparedHolder.Execute(const Params: array of const);
begin
  if not ready then exit;
  if not BindParametres(Params) then Exit;
  FReturnCode := sqlite3_step(vm);
  sqlite3_reset(vm);
  FOwner.DoPostExecute;
end;

procedure TSqlite3PreparedHolder.Disconnect;
begin
  if not ready then exit;
  sqlite3_finalize(vm);
end;

{$ifdef extra_prepared_thread_safe}
procedure TSqlite3PreparedHolder.Lock;
begin
  EnterCriticalsection(FRTI);
end;

procedure TSqlite3PreparedHolder.UnLock;
begin
  LeaveCriticalsection(FRTI);
end;
{$endif}

function TSqlite3PreparedHolder.Open(const Params: array of const) : Boolean;
var C, i : integer;
begin
  Result := false;
  if not ready then exit;
  if not BindParametres(Params) then Exit;

  FColumns.Clear;
  FReturnCode := sqlite3_step(vm);
  if (FReturnCode = SQLITE_ROW) then
  begin
    C := sqlite3_column_count(vm);
    if C > 0 then
    begin
      for i := 1 to C do FColumns.Add('');
      ConsumeColumns;
      Result := true;
    end;
  end;
end;

function TSqlite3PreparedHolder.Step: Boolean;
begin
  FReturnCode := sqlite3_step(vm);
  if (FReturnCode = SQLITE_ROW) then
  begin
    ConsumeColumns;
    Result := true;
  end else
    Result := false;
end;

procedure TSqlite3PreparedHolder.Close;
begin
  sqlite3_reset(vm);
end;

destructor TSqlite3PreparedHolder.Destroy;
begin
  {$ifdef extra_prepared_thread_safe}
  DoneCriticalsection(FRTI);
  {$endif}
  FColumns.Free;
  inherited Destroy;
end;

{ TExtSqlite3Dataset }

function TExtSqlite3Dataset.GenReturnString(aReturnCode: Cardinal): String;
begin
  FReturnCode:= aReturnCode;
  Result := ReturnString;
end;

procedure TExtSqlite3Dataset.ReconnectFunctions(aSqliteHandle : psqlite3);
var i : integer;
begin
  Lock;
  try
    if aSqliteHandle <> nil then
    begin
      For i := 0 to FFunctions.Count-1 do
      begin
        FFunctions[i].Reconnect(aSqliteHandle);
      end;
    end;
  finally
    UnLock;
  end;
end;

procedure TExtSqlite3Dataset.ReconnectPrepared(aSqliteHandle : psqlite3);
var i : integer;
begin
  Lock;
  try
    if aSqliteHandle <> nil then
    begin
      For i := 0 to FPrepared.Count-1 do
      begin
        FPrepared[i].Reconnect(aSqliteHandle);
      end;
    end;
  finally
    UnLock;
  end;
end;

class function TExtSqlite3Dataset.Sqlite3ExtFlagsToFFOO
                          (flags : TExtSqlite3OpenFlags) : Cardinal;
const cFFOAccord : Array [TExtSqlite3OpenFlag] of Cardinal = (
 SQLITE_OPEN_READONLY,
 SQLITE_OPEN_READWRITE,
 SQLITE_OPEN_CREATE,
 SQLITE_OPEN_DELETEONCLOSE,
 SQLITE_OPEN_EXCLUSIVE,
 SQLITE_OPEN_AUTOPROXY,
 SQLITE_OPEN_URI,
 SQLITE_OPEN_MEMORY,
 SQLITE_OPEN_MAIN_DB,
 SQLITE_OPEN_TEMP_DB,
 SQLITE_OPEN_TRANSIENT_DB,
 SQLITE_OPEN_MAIN_JOURNAL,
 SQLITE_OPEN_TEMP_JOURNAL,
 SQLITE_OPEN_SUBJOURNAL,
 $00004000, {SQLITE_OPEN_SUPER_JOURNAL}
 SQLITE_OPEN_NOMUTEX,
 SQLITE_OPEN_FULLMUTEX,
 SQLITE_OPEN_SHAREDCACHE,
 SQLITE_OPEN_PRIVATECACHE,
 SQLITE_OPEN_WAL,
 $01000000  {SQLITE_OPEN_NOFOLLOW} );
var it : TExtSqlite3OpenFlag;
begin
  Result := 0;
  for it := Low(TExtSqlite3OpenFlag) to High(TExtSqlite3OpenFlag) do
    if it in flags then
      Result := Result or cFFOAccord[it];
end;

function TExtSqlite3Dataset.InternalGetHandle: Pointer;
const
  CheckFileSql = 'Select Name from sqlite_master LIMIT 1';

var
  vm: Pointer;
  ErrorStr: String;
  lstValue : Pointer;
  Flags : Cardinal;
begin
  lstValue := FSqliteHandle;

  Flags := Sqlite3ExtFlagsToFFOO(FExtFlags);

  sqlite3_open_v2(PAnsiChar(FFileName), @Result, Flags, nil);
  //sqlite3_open returns SQLITE_OK even for invalid files
  //do additional check here
  FReturnCode := sqlite3_prepare_v2(Result, CheckFileSql, -1, @vm, nil);
  if FReturnCode <> SQLITE_OK then
  begin
    ErrorStr := SqliteCode2Str(FReturnCode) + ' - ' + sqlite3_errmsg(Result);
    sqlite3_close(Result);
    DatabaseError(ErrorStr, Self);
  end;
  sqlite3_finalize(vm);

  if Result <> lstValue then
  begin
    ReconnectFunctions(Result);
    ReconnectPrepared(Result);
  end;
end;

procedure TExtSqlite3Dataset.InternalCloseHandle;
var i : integer;
begin
  Lock;
  try
    if SqliteHandle <> nil then
    begin
      For i := 0 to FPrepared.Count-1 do
      begin
        FPrepared[i].Disconnect;
      end;
    end;
  finally
    UnLock;
  end;

  sqlite3_close(FSqliteHandle);
  FSqliteHandle := nil;
end;

procedure TExtSqlite3Dataset.InternalClose;
begin
  if Assigned(FUniDirPrepared) then
  begin
    sqlite3_finalize(FUniDirPrepared);
    FUniDirPrepared := nil;
  end;

  inherited InternalClose;
  FOpenMode := eomNormal;
  SetUniDirectional(false);
end;

constructor TExtSqlite3Dataset.Create(AOwner: TComponent);
begin
  inherited Create(AOwner);
  FOpenMode := eomNormal;
  FUniDirPrepared := nil;
  FPrepared := TSqlite3PrepCollection.Create;
  FFunctions := TSqlite3FuncCollection.Create;
  FDefaultStringSize := CustomSQLiteDS.DefaultStringSize;
  FOnPrepared := nil;
  FExtFlags := [sqlite_OpenReadWrite, sqlite_OpenCreate
                {$ifdef prepared_multi_holders}, sqlite_OpenNoMutex{$endif}
                ];
  InitCriticalSection(FRTI);
end;

procedure TExtSqlite3Dataset.Lock;
begin
  EnterCriticalsection(FRTI);
end;

procedure TExtSqlite3Dataset.UnLock;
begin
  LeaveCriticalsection(FRTI);
end;

procedure TExtSqlite3Dataset.AddFunction(aFunc: TSqlite3Function);
begin
  FFunctions.Add(aFunc);
  if FSqliteHandle <> nil then
     aFunc.Reconnect(FSqliteHandle);
end;

procedure TExtSqlite3Dataset.AddPrepared(aPrep : TSqlite3Prepared);
begin
  aPrep.FOwner := Self;
  FPrepared.Add(aPrep);
  if FSqliteHandle <> nil then
     aPrep.Reconnect(FSqliteHandle);
end;

function TExtSqlite3Dataset.AddNewPrep(const aSQL : String{$ifdef prepared_multi_holders};
  ThreadCnt : SmallInt{$endif}
  ) : TSqlite3Prepared;
begin
  Result := TSqlite3Prepared.Create(Self, aSQL{$ifdef prepared_multi_holders},
                                          ThreadCnt{$endif});
  FPrepared.Add(Result);
  if FSqliteHandle <> nil then
     Result.Reconnect(FSqliteHandle);
end;

procedure TExtSqlite3Dataset.ClearPrepared;
var i : integer;
begin
  for i := 0 to FPrepared.Count-1 do
     FPrepared[i].Disconnect;
  FPrepared.Clear;
end;

function TExtSqlite3Dataset.GetFieldData(Field: TField; Buffer: Pointer;
  NativeFormat: Boolean): Boolean;
var
  FieldRow: PAnsiChar;
  FieldOffset, Len: Integer;
begin
  if (FOpenMode = eomNormal) or
     (State in [dsCalcFields, dsInternalCalc, dsFilter]) then
      Result:=inherited GetFieldData(Field, Buffer, NativeFormat)
  else begin
    FieldOffset := Field.FieldNo - 1;

    Result := True;
    if (Buffer <> nil) then //supports GetIsNull
    begin
      case Field.Datatype of
      ftString:
        begin
          FieldRow := sqlite3_column_text(FUniDirPrepared, FieldOffset);
          Len := sqlite3_column_bytes(FUniDirPrepared, FieldOffset);
          if Len > 0 then
           Move(FieldRow^, PAnsiChar(Buffer)^, Len + 1) else
           PAnsiChar(Buffer)^ := #0;
        end;
      ftInteger, ftAutoInc:
        begin
          LongInt(Buffer^) := sqlite3_column_int(FUniDirPrepared, FieldOffset);
        end;
      ftBoolean, ftWord:
        begin
          Word(Buffer^) := Lo(sqlite3_column_int(FUniDirPrepared, FieldOffset));
        end;
      ftFloat, ftDateTime, ftTime, ftDate, ftCurrency:
        begin
          Double(Buffer^) := sqlite3_column_double(FUniDirPrepared, FieldOffset);
        end;
      ftLargeInt:
        begin
          Int64(Buffer^) := sqlite3_column_int64(FUniDirPrepared, FieldOffset);
        end;
      end;
    end;
  end;
end;

procedure TExtSqlite3Dataset.ExecuteDirect(const ASQL: String);
var
  vm: Pointer;
begin
  Lock;
  try
    FReturnCode := sqlite3_prepare_v2(FSqliteHandle, PAnsiChar(ASQL), -1, @vm, nil);
    DoRequestPrepared(ASQL, FReturnCode);
    if FReturnCode <> SQLITE_OK then
      DatabaseError(ReturnString, Self);
    FReturnCode := sqlite3_step(vm);
    sqlite3_finalize(vm);
  finally
    UnLock;
  end;
end;

function TExtSqlite3Dataset.QuickQuery(const ASQL: String;
  const AStrList: TStrings; FillObjects: Boolean): String;
var
  vm: Pointer;

  procedure FillStrings;
  begin
    while FReturnCode = SQLITE_ROW do
    begin
      AStrList.Add(String(sqlite3_column_text(vm,0)));
      FReturnCode := sqlite3_step(vm);
    end;
  end;
  procedure FillStringsAndObjects;
  begin
    while FReturnCode = SQLITE_ROW do
    begin
      AStrList.AddObject(String(sqlite3_column_text(vm, 0)),
        TObject(PtrInt(sqlite3_column_int(vm, 1))));
      FReturnCode := sqlite3_step(vm);
    end;
  end;
begin
  Lock;
  try
    if FSqliteHandle = nil then
      GetSqliteHandle;
    Result := '';
    FReturnCode := sqlite3_prepare_v2(FSqliteHandle,PAnsiChar(ASQL), -1, @vm, nil);
    DoRequestPrepared(ASQL, FReturnCode);
    if FReturnCode <> SQLITE_OK then
      DatabaseError(ReturnString, Self);

    FReturnCode := sqlite3_step(vm);
    if (FReturnCode = SQLITE_ROW) and (sqlite3_column_count(vm) > 0) then
    begin
      Result := String(sqlite3_column_text(vm, 0));
      if AStrList <> nil then
      begin
        if FillObjects and (sqlite3_column_count(vm) > 1) then
          FillStringsAndObjects
        else
          FillStrings;
      end;
    end;
    sqlite3_finalize(vm);
  finally
    UnLock;
  end;
end;

procedure TExtSqlite3Dataset.Open(aMode : TExtSqlite3OpenMode);
var DS : TDataSource;
begin
  if FOpenMode <> aMode then
  begin
    Close;
    FOpenMode := aMode;

    if FOpenMode = eomUniDirectional then
    begin
      SetUniDirectional(true);
      //need to reset all datalinks
      DS := DataSource;
      if assigned(DS) then
        DS.DataSet := nil;
    end;
  end;
  inherited Open;
end;

destructor TExtSqlite3Dataset.Destroy;
begin
  inherited Destroy;
  FFunctions.Free;
  FPrepared.Free;
  DoneCriticalsection(FRTI);
end;

function TExtSqlite3Dataset.SqliteExec(ASQL: PAnsiChar; ACallback: TSqliteCdeclCallback; Data: Pointer): Integer;
begin
  Result := sqlite3_exec(FSqliteHandle, ASQL, ACallback, Data, nil);
end;

procedure TExtSqlite3Dataset.RetrieveFieldDefs;
var
  ColumnStr: String;
  i, ColumnCount, DataSize: Integer;
  AType: TFieldType;
begin
  FAutoIncFieldNo := -1;
  FieldDefs.Clear;
  FReturnCode := sqlite3_prepare_v2(FSqliteHandle, PAnsiChar(FEffectiveSQL), -1,
                                                   @FUniDirPrepared, nil);
  DoRequestPrepared(FEffectiveSQL, FReturnCode);
  if FReturnCode <> SQLITE_OK then
    DatabaseError(ReturnString, Self);

  FReturnCode := sqlite3_step(FUniDirPrepared);
  ColumnCount := sqlite3_column_count(FUniDirPrepared);
  //Prepare the array of pchar2sql functions
  SetLength(FGetSqlStr, ColumnCount);
  for i := 0 to ColumnCount - 1 do
  begin
    DataSize := 0;
    ColumnStr := UpperCase(String(sqlite3_column_decltype(FUniDirPrepared, i)));
    if (ColumnStr = 'INTEGER') or (ColumnStr = 'INT') then
    begin
      if AutoIncrementKey and (UpperCase(String(sqlite3_column_name(FUniDirPrepared, i))) = UpperCase(PrimaryKey)) then
      begin
        AType := ftAutoInc;
        FAutoIncFieldNo := i;
      end
      else
        AType := ftInteger;
    end else if Pos('VARCHAR', ColumnStr) = 1 then
    begin
      AType := ftString;
      DataSize := StrToIntDef(Trim(ExtractDelimited(2, ColumnStr, ['(', ')'])), FDefaultStringSize);
    end else if (ColumnStr = 'TEXT') then
    begin
      AType := ftMemo;
    end else if (Pos('FLOAT', ColumnStr) = 1) or (Pos('NUMERIC', ColumnStr) = 1) or
     (Pos('REAL', ColumnStr) = 1)then
    begin
      AType := ftFloat;
    end else if Pos('AUTOINC', ColumnStr) = 1 then
    begin
      AType := ftAutoInc;
      if FAutoIncFieldNo = -1 then
        FAutoIncFieldNo := i;
    end else if (ColumnStr = 'DATETIME') then
    begin
      AType := ftDateTime;
    end else if (ColumnStr = 'DATE') then
    begin
      AType := ftDate;
    end else if (ColumnStr = 'LARGEINT') or (ColumnStr = 'BIGINT') then
    begin
      AType := ftLargeInt;
    end else if (ColumnStr = 'TIME') then
    begin
      AType := ftTime;
    end else if (ColumnStr = 'CURRENCY') then
    begin
      AType := ftCurrency;
    end else if (ColumnStr = 'WORD') then
    begin
      AType := ftWord;
    end else if Pos('BOOL', ColumnStr) = 1 then
    begin
      AType := ftBoolean;
    end  else if (ColumnStr = '') then
    begin
      case sqlite3_column_type(FUniDirPrepared, i) of
        SQLITE_INTEGER:
          AType := ftInteger;
        SQLITE_FLOAT:
          AType := ftFloat;
      else
        begin
          AType := ftString;
          DataSize := FDefaultStringSize;
        end;
      end;
    end else
    begin
      AType := ftString;
      DataSize := FDefaultStringSize;
    end;
    FieldDefs.Add(FieldDefs.MakeNameUnique(String(sqlite3_column_name(FUniDirPrepared, i))), AType, DataSize);
    //Set the pchar2sql function
    case AType of
      ftString:
        FGetSqlStr[i] := @Char2SQLStr;
      ftMemo:
        FGetSqlStr[i] := @Memo2SQLStr;
    else
      FGetSqlStr[i] := @Num2SQLStr;
    end;
  end;
  if (FOpenMode = eomNormal) then
  begin
    sqlite3_finalize(FUniDirPrepared);
    FUniDirPrepared := nil;
  end else
    sqlite3_reset(FUniDirPrepared);
end;

function TExtSqlite3Dataset.GetRowsAffected: Integer;
begin
  Result := sqlite3_changes(FSqliteHandle);
end;

procedure StrBufRealloc(var s : PAnsiChar; p: PAnsiChar; BufLen: Cardinal);
begin
  if (p = nil) or (p^ = #0) then
  begin
    if Assigned(s) then begin
      Freemem(pointer(s - sizeof(cardinal)));
      s := nil;
    end;
    Exit;
  end;
  if Assigned(s) then begin
    if (cardinal(pointer(s - sizeof(Cardinal))^) <
                           (BufLen + sizeof(Cardinal))) then
    begin
      Freemem(pointer(s - sizeof(Cardinal)));
      s := StrAlloc(BufLen);
    end;
  end else
    s := StrAlloc(BufLen);

  if assigned(s) then
    Move(p^, s^, BufLen);
end;

procedure TExtSqlite3Dataset.BuildLinkedList;
var
  TempItem: PDataRecord;
  {$IFNDEF CPU64}{$IFNDEF CPU32}
  Counter : Integer;
  {$ENDIF}{$ENDIF}
  ColumnCount: Integer;

procedure InitTempNextItem;
{$IFNDEF CPU64}{$IFNDEF CPU32}
var i : integer;
{$ENDIF}{$ENDIF}
begin
  New(TempItem^.Next);
  TempItem^.Next^.Previous := TempItem;
  TempItem := TempItem^.Next;
  GetMem(TempItem^.Row, FRowBufferSize);
  {$IFDEF CPU64}
  FillQWord(TempItem^.Row^, FRowCount, UIntPtr(nil));
  {$ELSE}
  {$IFDEF CPU32}
  FillDWord(TempItem^.Row^, FRowCount, UIntPtr(nil));
  {$ELSE}
  for i := 0 to FRowCount - 1 do
    TempItem^.Row[Counter] := nil;
  {$ENDIF}
  {$ENDIF}
end;

begin
  //Get AutoInc Field initial value
  if FAutoIncFieldNo <> -1 then
    sqlite3_exec(FSqliteHandle, PAnsiChar('Select Max(' + FieldDefs[FAutoIncFieldNo].Name +
      ') from ' + FTableName), @GetAutoIncValue, @FNextAutoInc, nil);

  if FOpenMode = eomNormal then
  begin
    FReturnCode := sqlite3_prepare_v2(FSqliteHandle, PAnsiChar(FEffectiveSQL), -1, @FUniDirPrepared, nil);
    if FReturnCode <> SQLITE_OK then
      DatabaseError(ReturnString, Self);
  end else
  begin
    if not Assigned(FUniDirPrepared) then
    begin
      FRecordCount := SQLITE_ERROR;
      DatabaseError(ReturnString, Self);
    end;
  end;

  FDataAllocated := True;

  TempItem := FBeginItem;
  FRecordCount := 0;
  ColumnCount := sqlite3_column_count(FUniDirPrepared);
  FRowCount := ColumnCount;
  //add extra rows for calculated fields
  if FCalcFieldList <> nil then
    Inc(FRowCount, FCalcFieldList.Count);
  FRowBufferSize := (SizeOf(PPAnsiChar) * FRowCount);
  if (FOpenMode = eomNormal) then
  begin
    FReturnCode := sqlite3_step(FUniDirPrepared);

    while FReturnCode = SQLITE_ROW do
    begin
      Inc(FRecordCount);
      InitTempNextItem;
      ConsumeRow(TempItem^.Row, ColumnCount);
      FReturnCode := sqlite3_step(FUniDirPrepared);
    end;

    sqlite3_finalize(FUniDirPrepared);
    FUniDirPrepared := nil;
  end else begin
    if FReturnCode = SQLITE_ROW then
      InitTempNextItem;
  end;
  // Attach EndItem
  TempItem^.Next := FEndItem;
  FEndItem^.Previous := TempItem;

  // Alloc temporary item used in edit
  GetMem(FSavedEditItem^.Row, FRowBufferSize);
  // Fill FBeginItem.Row with nil -> necessary for avoid exceptions in empty datasets
  GetMem(FBeginItem^.Row, FRowBufferSize);
  {$IFDEF CPU64}
  FillQWord(FBeginItem^.Row^, FRowCount, UIntPtr(nil));
  FillQWord(FSavedEditItem^.Row^, FRowCount, UIntPtr(nil));
  {$ELSE}
  {$IFDEF CPU32}
  FillDWord(FBeginItem^.Row^, FRowCount, UIntPtr(nil));
  FillDWord(FSavedEditItem^.Row^, FRowCount, UIntPtr(nil));
  {$ELSE}
  for Counter := 0 to FRowCount - 1 do
  begin
    FBeginItem^.Row[Counter] := nil;
    FSavedEditItem^.Row[Counter] := nil;
  end;
  {$ENDIF}
  {$ENDIF}
end;

procedure TExtSqlite3Dataset.ConsumeRow(aRecord : PPAnsiChar;
  ColumnCount : integer);
var Counter : integer;
begin
  for Counter := 0 to ColumnCount - 1 do
    StrBufRealloc(aRecord[Counter], sqlite3_column_text(FUniDirPrepared, Counter),
                                    sqlite3_column_bytes(FUniDirPrepared, Counter) + 1);
  //initialize calculated fields with nil
  {$IFDEF CPU64}
  FillQWord(aRecord[ColumnCount], (FRowCount - ColumnCount), UIntPtr(nil));
  {$ELSE}
  {$IFDEF CPU32}
  FillDWord(aRecord[ColumnCount], FRowCount - ColumnCount, UIntPtr(nil));
  {$ELSE}
  for Counter := ColumnCount to FRowCount - 1 do
    aRecord[Counter] := nil;
  {$ENDIF}
  {$ENDIF}
end;

procedure TExtSqlite3Dataset.SetDefaultStringSize(AValue: Integer);
begin
  if (FDefaultStringSize=AValue) or Active then Exit;
  FDefaultStringSize:=AValue;
end;

procedure TExtSqlite3Dataset.DoRequestPrepared(const aRequest: String;
  aResult: Integer);
begin
  if Assigned(FOnPrepared) then
    FOnPrepared(aRequest, aResult);
end;

function TExtSqlite3Dataset.GetRecord(Buffer: TRecordBuffer; GetMode: TGetMode; DoCheck: Boolean): TGetResult;
begin
  if FOpenMode = eomNormal then
    Result := inherited GetRecord(Buffer, GetMode, DoCheck)
  else
  begin
    FRecordCount := 0;
    Result := grOk;
    case GetMode of
      gmPrior:
        Result := grError;
      gmNext: begin
        FReturnCode := sqlite3_step(FUniDirPrepared);
        if not (FReturnCode = SQLITE_ROW) then
          Result := grEOF
      end;
    end; //case
    if Result = grOk then
    begin
      if assigned(Buffer) then begin
        PDataRecord(Pointer(Buffer)^) := FBeginItem^.Next;
        GetCalcFields(Buffer);
      end;
    end
      else if (Result = grError) and DoCheck then
        DatabaseError('No records found', Self);
  end;
end;

function TExtSqlite3Dataset.CreateBlobStream(Field: TField;
  Mode: TBlobStreamMode): TStream;
begin
  if FOpenMode = eomNormal then
    Result:=inherited CreateBlobStream(Field, Mode) else
  begin
    Result := TBufferedStream.Create;
    TBufferedStream(Result).SetPtr(sqlite3_column_text(FUniDirPrepared,  Field.FieldNo-1),
                                   sqlite3_column_bytes(FUniDirPrepared, Field.FieldNo-1));
  end;
end;

function TExtSqlite3Dataset.GetNextRecords : Longint;
begin
  if FOpenMode = eomUniDirectional then begin
    Result := 0;
    FRecordCount := 0;
    SetCurrentRecord(0);
    GetNextRecord;
  end else
    Result := inherited GetNextRecords;
end;

function TExtSqlite3Dataset.GetLastInsertRowId: Int64;
begin
  Lock;
  try
    Result := sqlite3_last_insert_rowid(FSqliteHandle);
  finally
    UnLock;
  end;
end;

function TExtSqlite3Dataset.ReturnString: String;
begin
  Lock;
  try
    Result := SqliteCode2Str(FReturnCode) + ' - ' + sqlite3_errmsg(FSqliteHandle);
  finally
    UnLock;
  end;
end;

class function TExtSqlite3Dataset.SqliteVersion: String;
begin
  Result := String(sqlite3_version());
end;

{ TSqlite3Function }

procedure TSqlite3Function.SetContext(context: psqlite3_context);
begin
  FContext := context;
end;

procedure TSqlite3Function.InternalScalarFunc(context: psqlite3_context;
  argc: integer; argv: ppsqlite3_value);
begin
  SetContext(context);
  FArguments:=argv;
  ScalarFunc(argc);
end;

procedure TSqlite3Function.InternalStepFunc(context: psqlite3_context;
  argc: integer; argv: ppsqlite3_value);
begin
  SetContext(context);
  FArguments:=argv;
  StepFunc(argc);
end;

procedure TSqlite3Function.InternalFinalFunc(context: psqlite3_context);
begin
  SetContext(context);
  FinalFunc();
end;

constructor TSqlite3Function.Create(const aName: String; aParCnt: Integer;
  aTextEncode: TSqlite3TextEncode; aFuncStyle: TSqlite3FuncStyle;
  isDeterministic: Boolean);
begin
  FName := aName;
  FFuncStyle := aFuncStyle;
  FTextEncode := aTextEncode;
  FDeterministic := isDeterministic;
  FParCnt:= aParCnt;
end;

destructor TSqlite3Function.Destroy;
begin
  inherited Destroy;
end;

function TSqlite3Function.GetAggregateContext(aSize: integer): Pointer;
begin
  Result := sqlite3_aggregate_context(FContext, aSize);
end;

procedure TSqlite3Function.SetResult(res: Variant);
begin
  case VarType(res) of
    varempty, varnull, varunknown : sqlite3_result_null(FContext);
    varsmallint, varinteger,
       varboolean, varshortint,
       varbyte, varword, varlongword : sqlite3_result_int(FContext, Integer(res));
    varstring, varustring, varolestr : sqlite3_result_text(FContext, pcharstr(res), length(res), @freebindstring);
    varsingle, vardouble : sqlite3_result_double(FContext, Double(res));
    varword64, varint64 : sqlite3_result_int64(FContext, Int64(res));
  end;
end;

function TSqlite3Function.GetArgument(arg: Integer): Variant;
var tp : Cardinal;
begin
  tp := sqlite3_value_type(FArguments[arg]);
  case tp of
    SQLITE_INTEGER  : Result := AsInt(arg);
    SQLITE_FLOAT    : Result := AsDouble(arg);
    //SQLITE_BLOB     :
    SQLITE_NULL     : Result := AsNull(arg);
    SQLITE3_TEXT    : Result := AsString(arg);
  end;
end;

function TSqlite3Function.AsInt(arg: Integer): Integer;
begin
  result := sqlite3_value_int(FArguments[arg]);
end;

function TSqlite3Function.AsInt64(arg: Integer): Int64;
begin
  result := sqlite3_value_int64(FArguments[arg]);
end;

function TSqlite3Function.AsDouble(arg: Integer): Double;
begin
  result := sqlite3_value_double(FArguments[arg]);
end;

function TSqlite3Function.AsString(arg: Integer): String;
begin
  result := StrPas(sqlite3_value_text(FArguments[arg]));
end;

function TSqlite3Function.AsNull({%H-}arg: integer): Pointer;
begin
  Result := nil;
end;

procedure TSqlite3Function.Reconnect(db: psqlite3);
var aEncode : Cardinal;
begin
  if FDeterministic then aEncode := SQLITE_DETERMINISTIC else aEncode := 0;
  aEncode := aEncode or GetSqlite3TextEncode(FTextEncode);
  if FFuncStyle = sqlfScalar then
  begin
    sqlite3_create_function_v2(db, pansichar(fName), FParCnt,
                                   aEncode, pointer(self),
                                   @scalar_func, nil, nil, @destroy_func);
  end else
  begin
    sqlite3_create_function_v2(db, pansichar(fName), FParCnt,
                                   aEncode, pointer(self),
                                   nil, @step_func, @final_func, @destroy_func);
  end;
end;

procedure TSqlite3Function.ScalarFunc({%H-}argc: integer);
begin
  //abstract
end;

procedure TSqlite3Function.StepFunc({%H-}argc: integer);
begin
  //abstract
end;

procedure TSqlite3Function.FinalFunc;
begin
  //abstract
end;

procedure TSqlite3Function.Disconnect;
begin
  //abstract
end;

end.


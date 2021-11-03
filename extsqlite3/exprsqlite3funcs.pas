{
 ExprSqlite3Funcs:
   Set of functions for expression processing to use with
   ExtSqlite3DS.TExtSqlite3Dataset

   Copyright (c) 2021 by Ilya Medvedkov

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
}


unit ExprSqlite3Funcs;

{$mode objfpc}{$H+}

interface

uses
  Classes, SysUtils,
  OGLFastNumList, ECommonObjs,
  ExprComparator, ExtSqlite3DS;

type
  { TExprCompareFunction }

  TExprCompareFunction = class(TSqlite3Function)
  public
    constructor Create;
    procedure ScalarFunc(argc : integer); override;
  end;

  { TExprContainFunction }

  TExprContainFunction = class(TSqlite3Function)
  public
    constructor Create;
    procedure ScalarFunc(argc : integer); override;
  end;

  { TExprCrossHitRateFunction }

  TExprCrossHitRateFunction = class(TSqlite3Function)
  public
    constructor Create;
    procedure ScalarFunc(argc : integer); override;
  end;

  { TExprCrossHitContFunction }

  TExprCrossHitContFunction = class(TSqlite3Function)
  public
    constructor Create;
    procedure ScalarFunc(argc : integer); override;
  end;

  { TExprRegFunction }

  TExprRegFunction = class(TSqlite3Function)
  public
    constructor Create;
    procedure ScalarFunc(argc : integer); override;
  end;

  { Block of indexed functions }

  TSinExprAdded = procedure (aKey : TKey; const aExpr : String);
  TSinExprDeleted = procedure (aKey : TKey);

  { TProtoSinExprs }

  TProtoSinExprs = class
  private
    function GetKeyField : String; virtual; abstract;
    function GetValField : String; virtual; abstract;
    function GetTblName  : String; virtual; abstract;
  public
    property KeyField : String read GetKeyField;
    property ValField : String read GetValField;
    property TblName : String read GetTblName;
  end;

  { TBaseSinExprs }

  TBaseSinExprs = class(TProtoSinExprs)
  private
    FTblName, FKeyField, FValField, FSinExprField : String;
    function GetKeyField : String; override;
    function GetValField : String; override;
    function GetTblName  : String; override;
    procedure InternalInitStates(const aTblName, aKeyField,
                                               aValField,
                                               aSinExprField : String); overload;
    procedure InternalInitStates(const aTblName, aKeyField,
                                               aValField : String); overload;
  private
    FDBDataset : TExtSqlite3Dataset;
    FUID : Cardinal;
  public
    constructor Create(aUID : Cardinal; aDB : TExtSqlite3Dataset);
    procedure InitStates(const aTblName, aKeyField, aValField: String); virtual; overload;
    procedure InitStates(const aTblName, aKeyField, aValField, aSinExprField: String); virtual; overload;
    property UID : Cardinal read FUID;
    property  SinExprField : String read FSinExprField;
  end;

  { TSinIndexedExprs }

  TSinIndexedExprs = class(specialize TFastKeyValuePairList<TSinExpr>)
  private
    FLock : TThreadSafeObject;
    function  GetValueSafe(aKey : TKey): TSinExpr;
    procedure SetValueSafe(aKey : TKey; const AValue: TSinExpr);
  protected
    procedure DisposeValue(Index : Integer); override;
    class function NullValue : TSinExpr; override;
  public
    constructor Create;
    destructor Destroy; override;
    procedure Lock;
    procedure UnLock;
    procedure AddExpr(aKey : TKey; const aExpr : String);
    procedure DeleteExpr(aKey : TKey);
    procedure AddKey(const aKey : TKey; const aValue : TSinExpr); override;
    procedure AddKeySorted(const aKey : TKey; const aValue : TSinExpr); override;
    property  ValueSafe[aKey : TKey] : TSinExpr read GetValueSafe write SetValueSafe;
  end;

  { TBaseSinIndexedExprs }

  TBaseSinIndexedExprs = class(TBaseSinExprs)
  private
    FExprs : TSinIndexedExprs;
    FAutoIncKey : TThreadSafeAutoIncrementCardinal;
    function GetValue(aKey : TKey) : TSinExpr;
    function GetValueSafe(aKey : TKey) : TSinExpr;
    procedure SetValue(aKey : TKey; AValue : TSinExpr);
    procedure SetValueSafe(aKey : TKey; AValue : TSinExpr);
  public
    constructor Create(aUID : Cardinal; aDB : TExtSqlite3Dataset);
    destructor Destroy; override;
    procedure Synchronize; virtual;
    procedure Lock;
    procedure UnLock;
    procedure AddExpr(aKey : TKey; const aExpr : String);
    function  AddAutoIncExpr(const aExpr : String) : Cardinal;
    procedure DeleteExpr(aKey : TKey);
    procedure Flush;
    procedure AddKey(const aKey : TKey; const aValue : TSinExpr);
    procedure AddKeySorted(const aKey : TKey; const aValue : TSinExpr);
    property  ValueSafe[aKey : TKey] : TSinExpr read GetValueSafe write SetValueSafe;
    property  Value[aKey : TKey] : TSinExpr read GetValue write SetValue;
  end;

  { TTokenedSinExprs }

  TTokenedSinExprs = class(TBaseSinIndexedExprs)
  private
    PREP_Synchronize : TSqlite3Prepared;
  public
    procedure InitStates(const aTblName, aKeyField,
                               aValField,
                               aSinExprField: String); override;
    procedure Synchronize; override;
  end;

  { TTokenSinFunction }

  TTokenSinFunction = class(TSqlite3Function)
  public
    constructor Create; overload;
    procedure ScalarFunc({%H-}argc : integer); override;
  end;

  { TSinConnectedIndexedExprs }

  TSinConnectedIndexedExprs = class(TBaseSinIndexedExprs)
  public
    procedure InitStates(const aTblName, aKeyField, aValField: String;
                               aRepopulate : Boolean); virtual; overload;
    procedure Repopulate;
  end;

  { TSinConnectedLogIndexedExprs }

  TSinConnectedLogIndexedExprs = class(TSinConnectedIndexedExprs)
  private
    FLogTable : String;
    FLastTimeStamp : Double;
    PREP_GetNow, PREP_Synchronize : TSqlite3Prepared;
  public
    procedure InitStates(const aTblName, aKeyField, aValField: String;
                               aRepopulate : Boolean); override;
    procedure Synchronize; override;
  end;

  { TTriggerIndexedFunction }

  TTriggerIndexedFunction = class(TSqlite3Function)
  private
    fexprs : TBaseSinIndexedExprs;
    fUID : Cardinal;
  public
    constructor Create(const aFunc: String; aParCnt: Integer;
      aUID : Cardinal;
      aexprs: TBaseSinIndexedExprs);
    property UID : Cardinal read FUID;
  end;

  { TUpdIndexedFunction }

  TUpdIndexedFunction = class(TTriggerIndexedFunction)
  public
    constructor Create(aUID: Cardinal; exprs: TBaseSinIndexedExprs);
    procedure ScalarFunc({%H-}argc : integer); override;
  end;

  { TInsIndexedFunction }

  TInsIndexedFunction = class(TTriggerIndexedFunction)
  public
    constructor Create(aUID: Cardinal; exprs: TBaseSinIndexedExprs);
    procedure ScalarFunc({%H-}argc : integer); override;
  end;

  { TDelIndexedFunction }

  TDelIndexedFunction = class(TTriggerIndexedFunction)
  public
    constructor Create(aUID: Cardinal; exprs: TBaseSinIndexedExprs);
    procedure ScalarFunc({%H-}argc : integer); override;
  end;

  { set of indexed functions }

  TExprFunction = class(TSqlite3Function)
  private
    procedure DoScalarFunc(argc : integer; E1, E2 : TSinExpr); virtual; abstract;
  end;

  { TExprIdxFunction }

  TExprIdxFunction = class(TExprFunction)
  private
    FIndexedExprs : TBaseSinIndexedExprs;
    FOwnIndexes : Boolean;
  public
    constructor Create(const aFunc: String; aParCnt: Integer;
      aIndexedExprs: TBaseSinIndexedExprs; aOwnIndexes: Boolean); overload;
    destructor Destroy; override;
    procedure ScalarFunc(argc : integer); override;
  end;

  { TExprCrossHitContIdxFunction }

  TExprCrossHitContIdxFunction = class(TExprIdxFunction)
  private
    procedure DoScalarFunc(argc : integer; E1, E2 : TSinExpr); override;
  public
    constructor Create(aIndexedExprs: TBaseSinIndexedExprs; aOwnIndexes: Boolean = false);
  end;

implementation

{ TTokenedSinExprs }

procedure TTokenedSinExprs.InitStates(const aTblName, aKeyField,
  aValField, aSinExprField : String);
var
  func : TSqlite3Function;
  idname : String;
begin
  inherited InitStates(aTblName, aKeyField, aValField, aSinExprField);
  if assigned(FDBDataset) then begin
    idname := aTblName+'_'+aValField+'_' + aSinExprField + '_' +inttostr(UID);

    func := FDBDataset.FindFunction(TTokenSinFunction, []);
    if not assigned(func) then
    begin
      FDBDataset.AddFunction(TTokenSinFunction.Create);
    end;

    FDBDataset.ExecSQL('DROP TRIGGER IF EXISTS update_' + idname);
    FDBDataset.ExecSQL('DROP TRIGGER IF EXISTS insert_' + idname);

    FDBDataset.ExecSQL('CREATE TEMP TRIGGER update_'+idname+
    ' AFTER UPDATE OF '+aValField+' ON '+aTblName+
    ' BEGIN '+
       'update '+aTblName+' set ' + aSinExprField + '=tosinexpr(NEW.'+aValField+') ' +
               'where '+aKeyField+'==OLD.'+aKeyField+';'+
    ' END;');

    FDBDataset.ExecSQL('CREATE TEMP TRIGGER insert_'+idname+
    ' AFTER INSERT ON '+aTblName+
    ' BEGIN '+
        'update '+aTblName+' set ' + aSinExprField + '=tosinexpr(NEW.'+aValField+') ' +
                'where '+aKeyField+'==NEW.'+aKeyField+';'+
    ' END;');

    PREP_Synchronize := FDBDataset.AddNewPrep(
    'update '+aTblName+' set ' + aSinExprField + '=tosinexpr('+aValField+') ' +
            'where '+aSinExprField+' is null;');
  end;
end;

procedure TTokenedSinExprs.Synchronize;
begin
  PREP_Synchronize.Execute;
end;

{ TTokenSinFunction }

constructor TTokenSinFunction.Create;
begin
  inherited Create('tosinexpr', 1, sqlteUtf8, sqlfScalar);
end;

procedure TTokenSinFunction.ScalarFunc(argc : integer);
var str_expr : String;
    expr : TSinExpr;
begin
  str_expr := AsString(0);
  expr := TSinExpr.Create(str_expr);
  try
    SetResult(expr.SaveToString);
  finally
    expr.Free;
  end;
end;

{ TBaseSinExprs }

function TBaseSinExprs.GetKeyField : String;
begin
  Result := FKeyField;
end;

function TBaseSinExprs.GetValField : String;
begin
  Result := FValField;
end;

function TBaseSinExprs.GetTblName : String;
begin
  Result := FTblName;
end;

procedure TBaseSinExprs.InternalInitStates(const aTblName, aKeyField,
  aValField, aSinExprField : String);
begin
  FTblName := aTblName;
  FKeyField := aKeyField;
  FValField := aValField;
  FSinExprField := aSinExprField;
end;

procedure TBaseSinExprs.InternalInitStates(const aTblName, aKeyField,
  aValField : String);
begin
  FTblName := aTblName;
  FKeyField := aKeyField;
  FValField := aValField;
  FSinExprField := '';
end;

constructor TBaseSinExprs.Create(aUID : Cardinal; aDB : TExtSqlite3Dataset);
begin
  inherited Create;
  FUID := aUID;
  FDBDataset := aDB;
end;

procedure TBaseSinExprs.InitStates(const aTblName, aKeyField,
  aValField : String);
begin
  InternalInitStates(aTblName, aKeyField, aValField);
end;

procedure TBaseSinExprs.InitStates(const aTblName, aKeyField, aValField,
  aSinExprField : String);
begin
  InternalInitStates(aTblName, aKeyField, aValField, aSinExprField);
end;

{ TBaseSinIndexedExprs }

function TBaseSinIndexedExprs.GetValue(aKey : TKey) : TSinExpr;
begin
  Result :=  FExprs.Value[aKey];
end;

function TBaseSinIndexedExprs.GetValueSafe(aKey : TKey) : TSinExpr;
begin
  Result :=  FExprs.ValueSafe[aKey];
end;

procedure TBaseSinIndexedExprs.SetValue(aKey : TKey; AValue : TSinExpr);
begin
  FExprs.Value[aKey] := AValue;
end;

procedure TBaseSinIndexedExprs.SetValueSafe(aKey : TKey; AValue : TSinExpr);
begin
  FExprs.ValueSafe[aKey] := AValue;
end;

constructor TBaseSinIndexedExprs.Create(aUID : Cardinal;
  aDB : TExtSqlite3Dataset);
begin
  inherited Create(aUID, aDB);
  FExprs := TSinIndexedExprs.Create;
  FAutoIncKey := TThreadSafeAutoIncrementCardinal.Create;
end;

destructor TBaseSinIndexedExprs.Destroy;
begin
  FExprs.Free;
  FAutoIncKey.Free;
  inherited Destroy;
end;

procedure TBaseSinIndexedExprs.Synchronize;
begin
  // donothing
end;

procedure TBaseSinIndexedExprs.Lock;
begin
  FExprs.Lock;
end;

procedure TBaseSinIndexedExprs.UnLock;
begin
  FExprs.UnLock;
end;

procedure TBaseSinIndexedExprs.AddExpr(aKey : TKey; const aExpr : String);
begin
  FExprs.AddExpr(aKey, aExpr);
end;

function TBaseSinIndexedExprs.AddAutoIncExpr(const aExpr : String) : Cardinal;
begin
  Result := FAutoIncKey.ID;
  AddExpr(Result, aExpr);
end;

procedure TBaseSinIndexedExprs.DeleteExpr(aKey : TKey);
begin
  FExprs.DeleteExpr(aKey);
end;

procedure TBaseSinIndexedExprs.Flush;
begin
  FExprs.Flush;
end;

procedure TBaseSinIndexedExprs.AddKey(const aKey : TKey; const aValue : TSinExpr
  );
begin
  FExprs.AddKey(aKey, aValue);
end;

procedure TBaseSinIndexedExprs.AddKeySorted(const aKey : TKey;
  const aValue : TSinExpr);
begin
  FExprs.AddKeySorted(aKey, aValue);
end;

{ TSinConnectedLogIndexedExprs }

procedure TSinConnectedLogIndexedExprs.InitStates(const aTblName, aKeyField,
  aValField : String; aRepopulate : Boolean);
var idname : String;
begin
  InternalInitStates(aTblName, aKeyField, aValField);
  if assigned(FDBDataset) then begin
    FLogTable := aTblName+'_log_'+aValField+'_'+inttostr(UID);
    idname := aTblName+'_'+aValField+'_'+inttostr(UID);

    PREP_GetNow := FDBDataset.AddNewPrep('select julianday("now")');

    FDBDataset.ExecuteDirect('create table if not exists '+FLogTable+' ('+
     'opid int,'+
     'refid int,'+
     'val text default null,'+
     'added double default (julianday(current_timestamp)))');

    PREP_Synchronize := FDBDataset.AddNewPrep('select opid,refid,val,'+
                     'max(added) as st from '+FLogTable+' where added > ?1 '+
                     'group by opid,refid order by st');

    FDBDataset.ExecSQL('DROP TRIGGER IF EXISTS updatelog_' + idname);
    FDBDataset.ExecSQL('DROP TRIGGER IF EXISTS insertlog_' + idname);
    FDBDataset.ExecSQL('DROP TRIGGER IF EXISTS deletelog_' + idname);

    FDBDataset.ExecSQL('CREATE TRIGGER updatelog_'+idname+
    ' AFTER UPDATE OF '+aValField+' ON '+aTblName+
    ' BEGIN '+
       'insert into '+FLogTable+' (opid, refid, val) values '+
               '(1, OLD.'+aKeyField+', NEW.'+aValField+');'+
    ' END;');

    FDBDataset.ExecSQL('CREATE TRIGGER insertlog_'+idname+
    ' AFTER INSERT ON '+aTblName+
    ' BEGIN '+
       'insert into '+FLogTable+' (opid, refid, val) values '+
               '(2, NEW.'+aKeyField+', NEW.'+aValField+');'+
    ' END;');

    FDBDataset.ExecSQL('CREATE TRIGGER deletelog_'+idname+
    ' AFTER DELETE ON '+aTblName+
    ' BEGIN '+
       'delete from '+FLogTable+' where opid!=3 and refid==OLD.'+aKeyField+';'+
       'insert into '+FLogTable+' (opid, refid) values '+
               '(3, OLD.'+aKeyField+');'+
    ' END;');

    PREP_GetNow.ExecToValue(@FLastTimeStamp, SizeOf(Double));
  end;
  if aRepopulate then Repopulate;
end;

procedure TSinConnectedLogIndexedExprs.Synchronize;
var opcode : Integer;
    refid : int64;
    stamp : Double;
begin
  if Assigned(FDBDataset) then
  with FDBDataset do
  begin
    PREP_Synchronize.Lock;
    try
      if PREP_Synchronize.OpenDirect([FLastTimeStamp]) then
      repeat
        opcode := PREP_Synchronize.AsInt32[0];
        refid  := PREP_Synchronize.AsInt64[1];
        case opcode of
          1, 2: ValueSafe[refid] := TSinExpr.Create(PREP_Synchronize.AsString[2]);
          3: DeleteExpr(refid);
        end;
        stamp := PREP_Synchronize.AsDouble[3];
        if FLastTimeStamp < stamp then
           FLastTimeStamp := stamp;
      until not PREP_Synchronize.Step;
      PREP_Synchronize.Close;
    finally
      PREP_Synchronize.UnLock;
    end;
  end;
end;

{ TSinConnectedIndexedExprs }

procedure TSinConnectedIndexedExprs.InitStates(const aTblName, aKeyField,
  aValField: String; aRepopulate: Boolean);
var idname : String;
begin
  InternalInitStates(aTblName, aKeyField, aValField);

  if assigned(FDBDataset) then begin
    FDBDataset.AddFunction(TUpdIndexedFunction.Create(UID, Self));
    FDBDataset.AddFunction(TInsIndexedFunction.Create(UID, Self));
    FDBDataset.AddFunction(TDelIndexedFunction.Create(UID, Self));

    idname := aTblName+'_'+aValField+'_'+inttostr(UID);

    FDBDataset.ExecSQL('DROP TRIGGER IF EXISTS update_' + idname);
    FDBDataset.ExecSQL('DROP TRIGGER IF EXISTS insert_' + idname);
    FDBDataset.ExecSQL('DROP TRIGGER IF EXISTS delete_' + idname);

    FDBDataset.ExecSQL('CREATE TEMP TRIGGER update_'+idname+
    ' AFTER UPDATE OF '+aValField+' ON '+aTblName+
    ' BEGIN '+
       'select updindexed'+inttostr(UID)+'(OLD.'+aKeyField+', NEW.'+aValField+');'+
    ' END;');

    FDBDataset.ExecSQL('CREATE TEMP TRIGGER insert_'+idname+
    ' AFTER INSERT ON '+aTblName+
    ' BEGIN '+
       'select insindexed'+inttostr(UID)+'(NEW.'+aKeyField+', NEW.'+aValField+');'+
    ' END;');

    FDBDataset.ExecSQL('CREATE TEMP TRIGGER delete_'+idname+
    ' AFTER DELETE ON '+aTblName+
    ' BEGIN '+
       'select delindexed'+inttostr(UID)+'(OLD.'+aKeyField+');'+
    ' END;');
  end;
  if aRepopulate then Repopulate;
end;

procedure TSinConnectedIndexedExprs.Repopulate;
begin
  Lock;
  try
    Flush;
    if Assigned(FDBDataset) then
    with FDBDataset do
    begin
      SQL := 'select '+fKeyField+', '+fValField+' from ' + fTblName;
      Open(eomUniDirectional);
      while not EOF do
      begin
        AddExpr(Fields[0].AsInteger, Fields[1].AsString);
        Next;
      end;
      Close;
    end;
  finally
    UnLock;
  end;
end;

{ TTriggerIndexedFunction }

constructor TTriggerIndexedFunction.Create(const aFunc: String;
  aParCnt: Integer; aUID: Cardinal; aexprs: TBaseSinIndexedExprs);
begin
  inherited Create(aFunc + inttostr(aUID), aParCnt, sqlteUtf8, sqlfScalar);
  fexprs := aexprs;
  fUID := aUID;
end;

{ TDelIndexedFunction }

constructor TDelIndexedFunction.Create(aUID: Cardinal; exprs: TBaseSinIndexedExprs);
begin
  inherited Create('delindexed', 1, aUID, exprs);
end;

procedure TDelIndexedFunction.ScalarFunc({%H-}argc: integer);
var id : Int64;
begin
  Id := AsInt64(0);
  fexprs.DeleteExpr(Id);
  SetResult(Id);
end;

{ TInsIndexedFunction }

constructor TInsIndexedFunction.Create(aUID: Cardinal; exprs: TBaseSinIndexedExprs);
begin
  inherited Create('insindexed', 2, aUID, exprs);
end;

procedure TInsIndexedFunction.ScalarFunc({%H-}argc: integer);
var id : Int64;
begin
  Id := AsInt64(0);
  fexprs.AddExpr(Id, AsString(1));
  SetResult(Id);
end;

{ TUpdIndexedFunction }

constructor TUpdIndexedFunction.Create(aUID: Cardinal; exprs: TBaseSinIndexedExprs);
begin
  inherited Create('updindexed', 2, aUID, exprs);
end;

procedure TUpdIndexedFunction.ScalarFunc({%H-}argc: integer);
var id : Int64;
begin
  Id := AsInt64(0);
  fexprs.ValueSafe[Id] := TSinExpr.Create(AsString(1));
  SetResult(Id);
end;

{ TExprCrossHitContIdxFunction }

procedure TExprCrossHitContIdxFunction.DoScalarFunc(argc: integer; E1,
  E2: TSinExpr);
begin
  if argc in [2..3] then
  begin
    if argc = 2 then
      SetResult( E1.CrossHitContain(E2) ) else
      SetResult( E1.CrossHitContain(E2, AsInt(2)) )
  end else
    SetResultNil;
end;

constructor TExprCrossHitContIdxFunction.Create(
  aIndexedExprs: TBaseSinIndexedExprs; aOwnIndexes: Boolean);
begin
  inherited Create('dlcrsconti', -1, aIndexedExprs, aOwnIndexes);
end;

{ TSinIndexedExprs }

function TSinIndexedExprs.GetValueSafe(aKey: TKey): TSinExpr;
begin
  Lock;
  try
    Result := inherited GetValue(aKey);
  finally
    UnLock;
  end;
end;

procedure TSinIndexedExprs.SetValueSafe(aKey: TKey; const AValue: TSinExpr);
begin
  Lock;
  try
    inherited SetValue(aKey, AValue);
  finally
    UnLock;
  end;
end;

procedure TSinIndexedExprs.DisposeValue(Index: Integer);
begin
  PKeyValuePair(FBaseList)[Index].Value.Free
end;

class function TSinIndexedExprs.NullValue: TSinExpr;
begin
  Result := nil;
end;

constructor TSinIndexedExprs.Create;
begin
  inherited Create(true);
  FLock := TThreadSafeObject.Create;
  Sort;
end;

destructor TSinIndexedExprs.Destroy;
begin
  FLock.Free;
  inherited Destroy;
end;

procedure TSinIndexedExprs.Lock;
begin
  FLock.Lock;
end;

procedure TSinIndexedExprs.UnLock;
begin
  FLock.UnLock;
end;

procedure TSinIndexedExprs.AddExpr(aKey: TKey; const aExpr: String);
begin
  AddKeySorted(aKey, TSinExpr.Create(aExpr));
end;

procedure TSinIndexedExprs.DeleteExpr(aKey: TKey);
var i : integer;
begin
  Lock;
  try
    i := IndexOfKey(aKey);
    if i >= 0 then Delete(i);
  finally
    UnLock;
  end;
end;

procedure TSinIndexedExprs.AddKey(const aKey: TKey; const aValue: TSinExpr);
begin
  // only sorted!
  AddKeySorted(aKey, aValue);
end;

procedure TSinIndexedExprs.AddKeySorted(const aKey: TKey; const aValue: TSinExpr
  );
begin
  Lock;
  try
    AddObjSorted(aKey, aValue);
  finally
    UnLock;
  end;
end;

{ TExprIdxFunction }

constructor TExprIdxFunction.Create(const aFunc : String; aParCnt : Integer;
                                           aIndexedExprs : TBaseSinIndexedExprs;
                                           aOwnIndexes : Boolean);
var s : String;
begin
  s := aFunc;
  if Assigned(aIndexedExprs) then
  begin
    s := s + '_' + aIndexedExprs.TblName + '_' + aIndexedExprs.ValField;
    if Length(aIndexedExprs.SinExprField) > 0 then
      s := s + '_' + aIndexedExprs.SinExprField;
  end;
  inherited Create(s, aParCnt, sqlteUtf8, sqlfScalar);
  FIndexedExprs := aIndexedExprs;
  FOwnIndexes := aOwnIndexes;
end;

destructor TExprIdxFunction.Destroy;
begin
  if FOwnIndexes and Assigned(FIndexedExprs) then
    FIndexedExprs.Free;

  inherited Destroy;
end;

procedure TExprIdxFunction.ScalarFunc(argc : integer);
var
  E1, E2 : TSinExpr;
  E1O, E2O : Boolean;
begin
  if (argc >= 2) then
  begin
    try
      E1 := nil; E2 := nil; E1O := true; E2O := true;
      try
        if assigned(FIndexedExprs) then
        begin
          case GetArgumentType(0) of
            sqlatInt, sqlatInt64 : begin
              E1 := FIndexedExprs.ValueSafe[AsInt(0)];
              E1O := false;
              if not Assigned(E1) then
              begin
                SetResultNil;
                Exit;
              end;
            end;
            sqlatString :
              E1 := TSinExpr.CreateFromData(AsPChar(0), False);
          end;
          case GetArgumentType(1) of
            sqlatInt, sqlatInt64 : begin
              E2 := FIndexedExprs.ValueSafe[AsInt(1)];
              E2O := false;
            end;
            sqlatString :
              E2 := TSinExpr.CreateFromData(AsPChar(1), false);
          end;
        end else begin
          E1 := TSinExpr.CreateFromData(AsPChar(0), false);
          E2 := TSinExpr.CreateFromData(AsPChar(1), false);
        end;
        DoScalarFunc(argc, E1, E2);
      except
        on e : Exception do SetResultNil;
      end;
    finally
      if Assigned(E1) and E1O then E1.Free;
      if Assigned(E2) and E2O then E2.Free;
    end;
  end else
    SetResultNil;
end;

{ TExprRegFunction }

constructor TExprRegFunction.Create;
begin
  inherited Create('regexpr', 2, sqlteUtf8, sqlfScalar);
end;

procedure TExprRegFunction.ScalarFunc(argc: integer);
var B : Boolean;
begin
  if (argc = 2) then begin
    B := SinRegExpr(AsString(0), AsString(1));
    SetResult(Integer(B));
  end else
    SetResultNil;
end;

{ TExprCrossHitContFunction }

constructor TExprCrossHitContFunction.Create;
begin
  inherited Create('dlcrscont', -1{2 or 3}, sqlteUtf8, sqlfScalar);
end;

procedure TExprCrossHitContFunction.ScalarFunc(argc: integer);
var
  E1, E2 : TSinExpr;
begin
  if argc in [2..3] then
  begin
    E1 := TSinExpr.Create(AsString(0));
    E2 := TSinExpr.Create(AsString(1));
    try
      if argc = 2 then
        SetResult( E1.CrossHitContain(E2) ) else
        SetResult( E1.CrossHitContain(E2, AsInt(2)) )
    finally
      E1.Free;
      E2.Free;
    end;
  end else
    SetResultNil;
end;

{ TExprCrossHitRateFunction }

constructor TExprCrossHitRateFunction.Create;
begin
  inherited Create('dlcrs', 2, sqlteUtf8, sqlfScalar);
end;

procedure TExprCrossHitRateFunction.ScalarFunc(argc: integer);
var
  E1, E2 : TSinExpr;
begin
  if argc = 2 then
  begin
    E1 := TSinExpr.Create(AsString(0));
    E2 := TSinExpr.Create(AsString(1));
    try
      SetResult( E1.CrossHitRate(E2) );
    finally
      E1.Free;
      E2.Free;
    end;
  end else
    SetResultNil;
end;

{ TExprContainFunction }

constructor TExprContainFunction.Create;
begin
  inherited Create('dlcont', 2, sqlteUtf8, sqlfScalar);
end;

procedure TExprContainFunction.ScalarFunc(argc: integer);
var
  E1, E2 : TSinExpr;
begin
  if argc = 2 then
  begin
    E1 := TSinExpr.Create(AsString(0));
    E2 := TSinExpr.Create(AsString(1));
    try
      SetResult( E1.Contain(E2) );
    finally
      E1.Free;
      E2.Free;
    end;
  end else
    SetResultNil;
end;

{ TExprCompareFunction }

constructor TExprCompareFunction.Create;
begin
  inherited Create('dldist', 2, sqlteUtf8, sqlfScalar);
end;

procedure TExprCompareFunction.ScalarFunc(argc: integer);
var
  E1, E2 : TSinExpr;
begin
  if argc = 2 then
  begin
    E1 := TSinExpr.Create(AsString(0));
    E2 := TSinExpr.Create(AsString(1));
    try
      SetResult( E1.Compare(E2) );
    finally
      E1.Free;
      E2.Free;
    end;
  end else
    SetResultNil;
end;

end.

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

  { TSinConnectedIndexedExprs }

  TSinConnectedIndexedExprs = class(TSinIndexedExprs)
  private
    FDBDataset : TExtSqlite3Dataset;
    FUID : Cardinal;
  public
    constructor Create(aUID : Cardinal; aDB : TExtSqlite3Dataset);
    procedure InitTriggers(const aTblName, aKeyField, aValField: String);

    property UID : Cardinal read FUID;
  end;

  { TTriggerIndexedFunction }

  TTriggerIndexedFunction = class(TSqlite3Function)
  private
    fexprs : TSinIndexedExprs;
    fUID : Cardinal;
  public
    constructor Create(const aFunc: String; aParCnt: Integer;
      aUID : Cardinal;
      aexprs: TSinIndexedExprs);
    property UID : Cardinal read FUID;
  end;

  { TUpdIndexedFunction }

  TUpdIndexedFunction = class(TTriggerIndexedFunction)
  public
    constructor Create(aUID: Cardinal; exprs: TSinIndexedExprs);
    procedure ScalarFunc({%H-}argc : integer); override;
  end;

  { TInsIndexedFunction }

  TInsIndexedFunction = class(TTriggerIndexedFunction)
  public
    constructor Create(aUID: Cardinal; exprs: TSinIndexedExprs);
    procedure ScalarFunc({%H-}argc : integer); override;
  end;

  { TDelIndexedFunction }

  TDelIndexedFunction = class(TTriggerIndexedFunction)
  public
    constructor Create(aUID: Cardinal; exprs: TSinIndexedExprs);
    procedure ScalarFunc({%H-}argc : integer); override;
  end;

  { TExprIdxFunction }

  TExprIdxFunction = class(TSqlite3Function)
  private
    FIndexedExprs : TSinIndexedExprs;
    FOwnIndexes : Boolean;
    procedure DoScalarFunc(argc : integer; E1, E2 : TSinExpr); virtual; abstract;
  public
    constructor Create(const aFunc: String; aParCnt: Integer;
      aIndexedExprs: TSinIndexedExprs; aOwnIndexes: Boolean); overload;
    destructor Destroy; override;
    procedure ScalarFunc(argc : integer); override;
  end;

  { TExprCrossHitContIdxFunction }

  TExprCrossHitContIdxFunction = class(TExprIdxFunction)
  private
    procedure DoScalarFunc(argc : integer; E1, E2 : TSinExpr); override;
  public
    constructor Create(aIndexedExprs: TSinIndexedExprs; aOwnIndexes: Boolean = false);
  end;


implementation

{ TSinConnectedIndexedExprs }

constructor TSinConnectedIndexedExprs.Create(aUID: Cardinal;
  aDB: TExtSqlite3Dataset);
begin
  inherited Create;
  FDBDataset := aDB;
  FUID := aUID;
end;

procedure TSinConnectedIndexedExprs.InitTriggers(const aTblName,
                                                       aKeyField,
                                                       aValField : String);
var idname : String;
begin
  FDBDataset.AddFunction(TUpdIndexedFunction.Create(FUID, Self));
  FDBDataset.AddFunction(TInsIndexedFunction.Create(FUID, Self));
  FDBDataset.AddFunction(TDelIndexedFunction.Create(FUID, Self));

  idname := aTblName+'_'+aValField+'_'+inttostr(UID);

  FDBDataset.ExecSQL('DROP TRIGGER IF EXISTS update_'+ idname);
  FDBDataset.ExecSQL('DROP TRIGGER IF EXISTS insert_'+ idname);
  FDBDataset.ExecSQL('DROP TRIGGER IF EXISTS delete_'+ idname);

  FDBDataset.ExecSQL('CREATE TRIGGER update_'+idname+
  ' AFTER UPDATE OF '+aValField+' ON '+aTblName+
  ' BEGIN '+
     'select updindexed'+inttostr(UID)+'(OLD.'+aKeyField+', NEW.'+aValField+');'+
  ' END;');

  FDBDataset.ExecSQL('CREATE TRIGGER insert_'+idname+
  ' AFTER INSERT ON '+aTblName+
  ' BEGIN '+
     'select insindexed'+inttostr(UID)+'(NEW.'+aKeyField+', NEW.'+aValField+');'+
  ' END;');

  FDBDataset.ExecSQL('CREATE TRIGGER delete_'+idname+
  ' AFTER DELETE ON '+aTblName+
  ' BEGIN '+
     'select delindexed'+inttostr(UID)+'(OLD.'+aKeyField+');'+
  ' END;');
end;

{ TTriggerIndexedFunction }

constructor TTriggerIndexedFunction.Create(const aFunc: String;
  aParCnt: Integer; aUID: Cardinal; aexprs: TSinIndexedExprs);
begin
  inherited Create(aFunc + inttostr(aUID), aParCnt, sqlteUtf8, sqlfScalar);
  fexprs := aexprs;
  fUID := aUID;
end;

{ TDelIndexedFunction }

constructor TDelIndexedFunction.Create(aUID: Cardinal; exprs: TSinIndexedExprs);
begin
  inherited Create('delindexed', 1, aUID, exprs);
end;

procedure TDelIndexedFunction.ScalarFunc({%H-}argc: integer);
begin
  fexprs.DeleteExpr(AsInt(0));
  SetResult(AsInt(0));
end;

{ TInsIndexedFunction }

constructor TInsIndexedFunction.Create(aUID: Cardinal; exprs: TSinIndexedExprs);
begin
  inherited Create('insindexed', 2, aUID, exprs);
end;

procedure TInsIndexedFunction.ScalarFunc({%H-}argc: integer);
begin
  fexprs.AddExpr(GetArgument(0), AsString(1));
  SetResult(AsInt(0));
end;

{ TUpdIndexedFunction }

constructor TUpdIndexedFunction.Create(aUID: Cardinal; exprs: TSinIndexedExprs);
begin
  inherited Create('updindexed', 2, aUID, exprs);
end;

procedure TUpdIndexedFunction.ScalarFunc({%H-}argc: integer);
begin
  fexprs.ValueSafe[AsInt(0)] := TSinExpr.Create(AsString(1));
  SetResult(AsInt(0));
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
    SetResult(null);
end;

constructor TExprCrossHitContIdxFunction.Create(
  aIndexedExprs: TSinIndexedExprs; aOwnIndexes: Boolean);
begin
  inherited Create('dlcrscontidx', -1, aIndexedExprs, aOwnIndexes);
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
                                           aIndexedExprs : TSinIndexedExprs;
                                           aOwnIndexes : Boolean);
begin
  inherited Create(aFunc, aParCnt, sqlteUtf8, sqlfScalar);
  FIndexedExprs := aIndexedExprs;
  FOwnIndexes := aOwnIndexes;
end;

destructor TExprIdxFunction.Destroy;
begin
  if FOwnIndexes and Assigned(FIndexedExprs) then
    FIndexedExprs.Free;

  inherited Destroy;
end;

procedure TExprIdxFunction.ScalarFunc(argc: integer);
var
  E1, E2 : TSinExpr;
begin
  if (argc >= 2) and Assigned(FIndexedExprs) then
  begin
    E1 := FIndexedExprs.Value[AsInt(0)];
    E2 := FIndexedExprs.Value[AsInt(1)];
    if Assigned(E1) and Assigned(E2) then
      DoScalarFunc(argc, E1, E2) else
      SetResult(null);
  end else
    SetResult(null);
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
    SetResult(null);
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
    SetResult(null);
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
    SetResult(null);
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
    SetResult(null);
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
    SetResult(null);
end;

end.

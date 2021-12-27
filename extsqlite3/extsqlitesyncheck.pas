{
 ExtSqliteSynCheck:
   Sqlite Statements Syntax Parser

   Copyright (c) 2021 by Ilya Medvedkov

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
}

unit ExtSqliteSynCheck;

{$mode objfpc}{$H+}

interface

uses
  Classes, SysUtils,
  ECommonObjs, OGLFastList, OGLFastNumList,
  OGLRegExprWrapper,
  ExtSqliteTokens;

type

  TSqliteStmt = (stmtUnknown,
                 stmtPragma,
                 stmtRelease,
                 stmtRollback,
                 stmtSavepoint,
                 stmtCommit,
                 stmtBegin,
                 stmtSelect,
                 stmtInsert,
                 stmtUpdate,
                 stmtUpsert,
                 stmtReplace,
                 stmtDelete,
                 stmtAlterTable,
                 stmtCreateTrigger,
                 stmtCreateTable,
                 stmtCreateIndex,
                 stmtCreateView,
                 stmtCreateVtable,
                 stmtDropIndex,
                 stmtDropTable,
                 stmtDropTrigger,
                 stmtDropView,
                 stmtAnalize);

  { TSqliteSynRule }

  TSqliteSynRule = class
  private
    FReadOnly : Boolean;
    FKind : TSqliteStmt;
    FRule : AnsiString;
    FRegExpr : AnsiString;
    FWRegExpr : TRegExprWrapper;
  public
    constructor Create(aKind : TSqliteStmt; const aRule : String;
      aReadOnly : Boolean);
    destructor Destroy; override;
    function Check(const S : String) : Boolean;
    function GenRegExpr : String;
    property ReadOnly : Boolean read FReadOnly;
    property Kind : TSqliteStmt read FKind;
  end;

  { TSqliteSynRules }

  TSqliteSynRules = class
  private
    FExprTree : Array [TSqliteStmt] of TSqliteSynRule;
  public
    constructor Create;
    destructor Destroy; override;

    class function GenerateSynCheckExpr(aExpr : TSqliteExpr) : String;

    procedure AddRule(aName : TSqliteStmt;
                       const aRule: String; aRO: Boolean);

    function CheckExpr(aRule : TSqliteStmt;
                       const aExpr : String) : Boolean; overload;
    function CheckExpr(aRule : TSqliteStmt;
                         aExpr : TSqliteExpr) : Boolean; overload;
    function CheckExpr(aRule : TSqliteSynRule;
                         aExpr : TSqliteExpr) : Boolean; overload;
    function FindRuleForExpr(aExpr : TSqliteExpr) : TSqliteSynRule;
  end;

  { TSqliteSynAnalizer }

  TSqliteSynAnalizer = class
  protected
    FErrorToken : TSqliteToken;
    FErrorPar  : String;
    FErrorCode : Integer;
    FSynExpr : TSqliteExpr;
  private
    class procedure pGoNextNonspace(aExpr : TSqliteExpr; var i : Integer;
      var T : TSqliteToken);
    class procedure pChangeStateGoNextNonspace(aExpr : TSqliteExpr; nst : Byte;
      var State : Byte; var i : Integer; var T : TSqliteToken);
    class function pCheckKeyword(T : TSqliteToken; kw : Cardinal) : Boolean;
    class function pCheckStatementEnd(T : TSqliteToken) : Boolean;
    class function pCheckId(T : TSqliteToken; maxlen : Integer) : Boolean;
    function GetErrorPos : Integer;
    function GetErrorStr : String;
    function GetErrorTokenValue : String;
  public
    constructor Create(aExpr : TSqliteExpr);

    class function CheckStmt(Kind : TSqliteStmt; aExpr : TSqliteExpr) : Boolean;
    class function CheckIsCreateTable(aExpr : TSqliteExpr) : Boolean;
    class function CheckIsBegin(aExpr : TSqliteExpr) : Boolean;
    class function CheckIsCommit(aExpr : TSqliteExpr) : Boolean;
    class function CheckIsSavepoint(aExpr : TSqliteExpr) : Boolean;
    class function CheckIsRelease(aExpr : TSqliteExpr) : Boolean;
    class function CheckIsRollback(aExpr : TSqliteExpr) : Boolean;

    property ErrorCode : Integer read FErrorCode;
    property ErrorPos : Integer read GetErrorPos;
    property ErroredToken : TSqliteToken read FErrorToken;
    property ErrorStr : String read GetErrorStr;
    property ErrorTokenValue : String read GetErrorTokenValue;
  end;

function SqliteSynRules : TSqliteSynRules;
procedure SetSqliteSynRules(aRules : TSqliteSynRules);

const SAER_NOERROR              = 0;
      SAER_WAIT_TO_RUN          = -1;
      SAER_SYNEXPR_NOT_ASSIGNED = -10;
      SAER_SYNEXPR_EMPTY        = -20;
      SAER_UNEXPECTED           = -30;
      SAER_NOT_SUPPORTED        = -40;
      SAER_ALREADY_EXISTS       = -50;
      SAER_PK_ONLY_INTEGER      = -60;
      SAER_NO_SUCH_COLUMN       = -70;
      SAER_NO_SUCH_TABLE        = -80;
      SAER_KEYWORD_EXPECTED     = $100;
      SAER_IDENTIFIER_EXPECTED  = $200;
      SAER_SYMBOL_EXPECTED      = $400;
      SAER_VALUE_EXPECTED       = $800;

implementation

uses
  LazUTF8, ExtSqliteUtils;

const cSqliteErrorMsg    = 'Error ''%s'' detected at pos %d ''%s''';
      cSqliteNoErrorMsg  = 'No error';
      cSqlite_SAER_WAIT_TO_RUN          = 'Waiting for run';
      cSqlite_SAER_SYNEXPR_NOT_ASSIGNED = 'Syntax expression not assigned';
      cSqlite_SAER_SYNEXPR_EMPTY        = 'Syntax expression is empty';
      cSqlite_SAER_UNEXPECTED           = 'Unexpected';
      cSqlite_SAER_NOT_SUPPORTED        = 'Not supported';
      cSqlite_SAER_ALREADY_EXISTS       = 'already exists';
      cSqlite_SAER_KEYWORD_EXPECTED     = '%s keyword expected';
      cSqlite_SAER_ANY_KEYWORD_EXPECTED = 'suitable keyword expected';
      cSqlite_SAER_IDENTIFIER_EXPECTED  = 'identifier expected';
      cSqlite_SAER_SYMBOL_EXPECTED      = 'symbol %s expected';
      cSqlite_SAER_ANY_SYMBOL_EXPECTED  = 'suitable symbol expected';
      cSqlite_SAER_VALUE_EXPECTED       = 'suitable value expected' ;
      cSqlite_SAER_PK_ONLY_INTEGER      = 'Primary key is only allowed for integer columns';
      cSqlite_SAER_NO_SUCH_COLUMN       = 'No such column(s) %s';
      cSqlite_SAER_NO_SUCH_TABLE        = 'No such table %s';

var vSynRules : TSqliteSynRules = nil;

function SqliteSynRules : TSqliteSynRules;
begin
  Result := vSynRules;
end;

procedure SetSqliteSynRules(aRules : TSqliteSynRules);
begin
  if Assigned(vSynRules) then vSynRules.Free;
  vSynRules := aRules;
end;

{ TSqliteSynRule }

constructor TSqliteSynRule.Create(aKind : TSqliteStmt; const aRule : String;
  aReadOnly : Boolean);
begin
  FKind := aKind;
  FRule := aRule;
  FWRegExpr := nil;
  FRegExpr := GenRegExpr;
  FReadOnly := aReadOnly;
end;

destructor TSqliteSynRule.Destroy;
begin
  if Assigned(FWRegExpr) then FWRegExpr.Free;
  inherited Destroy;
end;

function TSqliteSynRule.Check(const S : String) : Boolean;
begin
  Result := FWRegExpr.Exec(S);
end;

function TSqliteSynRule.GenRegExpr : String;
begin
  //todo : design simple representation for sqlite syn rules
  //       for example:
  //       from rule
  //         CREATE [TEMP,TEMPORARY] TABLE [IF NOT EXISTS] id1-2 (id1 id1*)
  //       we must generate regexpr
  //         CREATE( TEMP| TEMPORARY){0,1} TABLE( IF NOT EXISTS){0,1} (id1|id2) \( id1 (id1 )*\)
  Result := FRule;
  if assigned(FWRegExpr) then
    FWRegExpr.Free;
  FWRegExpr := TRegExprWrapper.Create(Result);
end;

{ TSqliteSynRules }

constructor TSqliteSynRules.Create;
begin
  inherited Create;
  {$ifdef CPU64}
  FillQWord(FExprTree, Length(FExprTree), Qword(nil));
  {$else}
  FillDWord(FExprTree, Length(FExprTree), Dword(nil));
  {$endif}
end;

destructor TSqliteSynRules.Destroy;
var i : TSqliteStmt;
begin
  for i := Low(TSqliteStmt) to High(TSqliteStmt) do
  if assigned(FExprTree[i]) then
  begin
    FExprTree[i].Free;
  end;
  inherited Destroy;
end;

class function TSqliteSynRules.GenerateSynCheckExpr(aExpr : TSqliteExpr
  ) : String;
var i       : integer;
    lstKind : TSqliteTokenKind;
begin
  lstKind := stkUnknown;
  Result := '';
  for i := 0 to aExpr.Count-1 do
  begin
    if (i > 0) and
       (lstKind <> aExpr[i].Kind) and
       (aExpr[i].Kind <> stkSpace) and
       (lstKind <> stkSpace) then
    begin
      Result := Result + ' ';
    end;
    case aExpr[i].Kind of
      stkSpace : Result := Result + ' ';
      stkIdentifier : begin
        Result := Result + 'id' + Inttostr(aExpr[i].IdCnt);
      end;
      stkKeyWord :
        Result := Result + aExpr[i].QuotedToken;
      stkString :
        Result := Result + 'str';
      stkNumber :
        Result := Result + 'num';
      stkSymbol :
        Result := Result + aExpr[i].Token;
    end;
    lstKind := aExpr[i].Kind;
  end;
end;

procedure TSqliteSynRules.AddRule(aName : TSqliteStmt; const aRule : String;
  aRO : Boolean);
var R : TSqliteSynRule;
begin
  R := TSqliteSynRule.Create(aName, aRule, aRO);
  if Assigned(FExprTree[aName]) then FExprTree[aName].Free;
  FExprTree[aName] := R;
end;

function TSqliteSynRules.CheckExpr(aRule : TSqliteStmt; const aExpr : String
  ) : Boolean;
var R : TSqliteSynRule;
    lExpr : TSqliteExpr;
begin
  R := FExprTree[aRule];
  if Assigned(R) then
  begin
    lExpr := TSqliteExpr.Create(aExpr);
    try
      Result := CheckExpr(R, lExpr);
    finally
      lExpr.Free;
    end;
  end else
    Result := false
end;

function TSqliteSynRules.CheckExpr(aRule : TSqliteStmt; aExpr : TSqliteExpr
  ) : Boolean;
var R : TSqliteSynRule;
begin
  R := FExprTree[aRule];
  if Assigned(R) then
    Result := CheckExpr(R, aExpr) else
    Result := false;
end;

function TSqliteSynRules.CheckExpr(aRule : TSqliteSynRule; aExpr : TSqliteExpr
  ) : Boolean;
var
  S : String;
begin
  S := GenerateSynCheckExpr(aExpr);
  Result := aRule.Check(S);
end;

function TSqliteSynRules.FindRuleForExpr(aExpr : TSqliteExpr) : TSqliteSynRule;
var i : TSqliteStmt;
    S : String;
begin
  S := GenerateSynCheckExpr(aExpr);
  for i := Low(TSqliteStmt) to High(TSqliteStmt) do
  if Assigned(FExprTree[i]) then
  begin
    if FExprTree[i].Check(S) then
      Exit(FExprTree[i]);
  end;
  Result := nil;
end;

{ TSqliteSynAnalizer }

function TSqliteSynAnalizer.GetErrorPos : Integer;
begin
  if Assigned(FErrorToken) then
    Result := FErrorToken.Pos else
    Result := 0;
end;

function TSqliteSynAnalizer.GetErrorStr : String;
var S : String;
begin
  if FErrorCode = 0 then
  begin
    Result := cSqliteNoErrorMsg;
  end else begin
    if ErrorCode > 0 then
    begin
      S := '';
      if (ErrorCode and SAER_SYMBOL_EXPECTED) > 0 then
      begin
        if ErrorCode and $ff = 0 then S := cSqlite_SAER_ANY_SYMBOL_EXPECTED else
         S := Format(cSqlite_SAER_SYMBOL_EXPECTED, [String(Chr(ErrorCode and $ff))]);
      end;
      if (ErrorCode and SAER_KEYWORD_EXPECTED) > 0 then
      begin
        if Length(S) > 0 then S := S + ' / ';
        if ErrorCode and $ff = 0 then S := S + cSqlite_SAER_ANY_KEYWORD_EXPECTED else
         S := S + Format(cSqlite_SAER_KEYWORD_EXPECTED, [sqluGetIndexedKeyWord(ErrorCode and $ff-1)]);
      end;
      if (ErrorCode and SAER_IDENTIFIER_EXPECTED) > 0 then
      begin
        if Length(S) > 0 then S := S + ' / ';
        S := S + cSqlite_SAER_IDENTIFIER_EXPECTED;
      end;
      if (ErrorCode and SAER_VALUE_EXPECTED) > 0 then
      begin
        if Length(S) > 0 then S := S + ' / ';
        S := S + cSqlite_SAER_VALUE_EXPECTED;
      end;
    end else
    begin
      if ErrorCode = SAER_WAIT_TO_RUN then S := cSqlite_SAER_WAIT_TO_RUN else
      if ErrorCode = SAER_SYNEXPR_NOT_ASSIGNED then S := cSqlite_SAER_SYNEXPR_NOT_ASSIGNED else
      if ErrorCode = SAER_SYNEXPR_EMPTY then S := cSqlite_SAER_SYNEXPR_EMPTY else
      if ErrorCode = SAER_UNEXPECTED then S := cSqlite_SAER_UNEXPECTED else
      if ErrorCode = SAER_NOT_SUPPORTED then S := cSqlite_SAER_NOT_SUPPORTED else
      if ErrorCode = SAER_ALREADY_EXISTS then S := cSqlite_SAER_ALREADY_EXISTS else
      if ErrorCode = SAER_PK_ONLY_INTEGER then S := cSqlite_SAER_PK_ONLY_INTEGER else
      if ErrorCode = SAER_NO_SUCH_COLUMN then begin
        if Length(FErrorPar) > 0 then
          S := Format(cSqlite_SAER_NO_SUCH_COLUMN, [FErrorPar])
        else
        if Assigned(FErrorToken) then
          S := Format(cSqlite_SAER_NO_SUCH_COLUMN, [FErrorToken.QuotedToken]) else
          S := Format(cSqlite_SAER_NO_SUCH_COLUMN, [''])
      end else
      if ErrorCode = SAER_NO_SUCH_TABLE then begin
        if Length(FErrorPar) > 0 then
          S := Format(cSqlite_SAER_NO_SUCH_TABLE, [FErrorPar])
        else
        if Assigned(FErrorToken) then
          S := Format(cSqlite_SAER_NO_SUCH_TABLE, [FErrorToken.QuotedToken]) else
          S := Format(cSqlite_SAER_NO_SUCH_TABLE, [''])
      end else
      S := '';
    end;
    Result := Format(cSqliteErrorMsg, [S, ErrorPos, ErrorTokenValue]);
  end;
end;

function TSqliteSynAnalizer.GetErrorTokenValue : String;
begin
  if Assigned(ErroredToken) then
    Result := ErroredToken.Token else
    Result := '';
end;

constructor TSqliteSynAnalizer.Create(aExpr : TSqliteExpr);
begin
  FSynExpr := aExpr;
  FErrorCode := SAER_WAIT_TO_RUN;
  FErrorPar := '';
  FErrorToken := nil;
end;

class procedure TSqliteSynAnalizer.pGoNextNonspace(aExpr : TSqliteExpr;
                     var i : Integer; var T : TSqliteToken);
var ind : integer;
begin
  for ind := i+1 to aExpr.count-1 do
  begin
    Inc(i);
    T := aExpr[i];
    if T.Kind <> stkSpace then begin
      Exit;
    end;
  end;
end;

class procedure TSqliteSynAnalizer.pChangeStateGoNextNonspace(
                     aExpr : TSqliteExpr;
                     nst : Byte;
                     var State : Byte;
                     var i : Integer;
                     var T : TSqliteToken);
begin
  State := nst;
  pGoNextNonspace(aExpr, i, T);
end;

class function TSqliteSynAnalizer.pCheckKeyword(T : TSqliteToken;
                     kw : Cardinal) : Boolean;
begin
  Result := (T.Kind = stkKeyWord) and
            (T.KeyWordIndx = kw);
end;

class function TSqliteSynAnalizer.pCheckStatementEnd(T : TSqliteToken
  ) : Boolean;
begin
  Result := (T.Kind = stkSymbol) and
            (T.SymbolOrd = STOK_SEMICOLUMN);
end;

class function TSqliteSynAnalizer.pCheckId(T : TSqliteToken; maxlen : Integer
  ) : Boolean;
begin
  Result := (T.Kind = stkIdentifier) and
            (T.IdCnt <= maxlen);
end;

class function TSqliteSynAnalizer.CheckStmt(Kind : TSqliteStmt;
  aExpr : TSqliteExpr) : Boolean;
begin
  Result := vSynRules.CheckExpr(Kind, aExpr);
end;

class function TSqliteSynAnalizer.CheckIsCreateTable(aExpr : TSqliteExpr
  ) : Boolean;
begin
  Result := vSynRules.CheckExpr(stmtCreateTable, aExpr);
end;

class function TSqliteSynAnalizer.CheckIsBegin(aExpr : TSqliteExpr) : Boolean;
begin
  Result := vSynRules.CheckExpr(stmtBegin, aExpr);
end;

class function TSqliteSynAnalizer.CheckIsCommit(aExpr : TSqliteExpr) : Boolean;
begin
  Result := vSynRules.CheckExpr(stmtCommit, aExpr);
end;

class function TSqliteSynAnalizer.CheckIsSavepoint(aExpr : TSqliteExpr
  ) : Boolean;
begin
  Result := vSynRules.CheckExpr(stmtSavepoint, aExpr);
end;

class function TSqliteSynAnalizer.CheckIsRelease(aExpr : TSqliteExpr) : Boolean;
begin
  Result := vSynRules.CheckExpr(stmtRelease, aExpr);
end;

class function TSqliteSynAnalizer.CheckIsRollback(aExpr : TSqliteExpr
  ) : Boolean;
begin
  Result := vSynRules.CheckExpr(stmtRollback, aExpr);
end;

initialization
  vSynRules := TSqliteSynRules.Create;
  //todo : move to ext file
  vSynRules.AddRule(stmtRelease,
                    'RELEASE( SAVEPOINT){0,1} id1(;|$)', true);
  vSynRules.AddRule(stmtSavepoint,
                    'SAVEPOINT id1(;|$)', true);
  vSynRules.AddRule(stmtCommit,
                    '(COMMIT|END){1}( TRANSACTION){0,1}(;|$)', true);
  vSynRules.AddRule(stmtBegin,
                    'BEGIN( DEFERRED| IMMEDIATE| EXCLUSIVE){0,1}( TRANSACTION){0,1}(;|$)', true);
  vSynRules.AddRule(stmtRollback,
                    'ROLLBACK( TRANSACTION){0,1}( TO( SAVEPOINT){0,1} id1){0,1}(;|$)', false);
  vSynRules.AddRule(stmtCreateTable,
                    'CREATE( TEMP| TEMPORARY){0,1} TABLE( IF NOT EXISTS){0,1} (id1|id2) \( .+ \)(( WITHOUT ROWID| STRICT){1}( ,){0,1}){0,2}(;|$)', false);
  vSynRules.AddRule(stmtSelect,
                    '^(WITH ((.+) AS( NOT){0,1}( MATERIALIZED){0,1} \( (SELECT |VALUES ){1}.+ \)(\,){0,1} )+){0,1}(SELECT |VALUES ){1}(.+)(;|$)', true);

finalization
  if assigned(vSynRules) then
    vSynRules.Free;

end.


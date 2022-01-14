{
 ExtSqliteTokens:
   Sqlite Token and Expression Extractor

   Copyright (c) 2021 by Ilya Medvedkov

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
}


unit ExtSqliteTokens;

{$mode objfpc}{$H+}

interface

uses
  Classes, SysUtils, OGLFastList, OGLRegExprWrapper, ExtSqliteUtils;

type
  TSqliteToken = class;

  TSqliteTokenKind = (stkUnknown,
                      stkKeyWord,
                      stkDataType,
                      stkSymbol, stkSpace,
                      stkIdentifier, stkTableName, stkFieldName,
                      stkNumber, stkString);
  TSqliteTokenKinds = set of TSqliteTokenKind;

  TSqliteTokens = class(specialize TFastBaseCollection<TSqliteToken>);

  { TSqliteToken }

  TSqliteToken = class
  private
    FSubTokens : TSqliteTokens;
    FPos   : Integer;
    FLen   : Integer;
    FKind  : TSqliteTokenKind;
    FToken : String;
    FTag   : Array [0..3] of Byte;
    function GetIsQuoted : Boolean;
    function GetQuotedToken : String;
    function GetSubToken(Index : Integer): TSqliteToken;
    function GetSubTokenCnt: Integer;
    function GetSubTokenStr(Index : Integer) : String;
    function GetTag(Index : Integer) : Byte;
  public
    constructor Create(const aToken : String; aKind : TSqliteTokenKind); overload;
    constructor Create(aToken : TSqliteToken); overload;
    destructor Destroy; override;
    procedure AddSubToken(sT : TSqliteToken);
    procedure AssignToken(aToken : TSqliteToken);
    property SubTokenCnt : Integer read GetSubTokenCnt;
    property IsQuoted : Boolean read GetIsQuoted;
    property SubTokenStr[Index : Integer] : String read GetSubTokenStr;
    property SubToken[Index : Integer] : TSqliteToken read GetSubToken;
    property QuotedToken : String read GetQuotedToken;
    property Token : String read FToken;
    function Compare(aToken : TSqliteToken) : Boolean;

    property Kind : TSqliteTokenKind read FKind;
    property Len : Integer read FLen;
    property Pos : Integer read FPos;

    property Tag[Index : Integer] : Byte read GetTag;
    function KeyWordIndx : Byte;
    function SymbolOrd : Byte;
    function IdCnt : Byte;
    function TagInt : Int32;
  end;

  { TSqliteExpr }

  TSqliteExpr = class(TSqliteTokens)
  private
    FOrigExpr : String;
    FPos, FLen : Integer;
    FLastNSToken : TSqliteToken;

    procedure AddTokenStr(const aToken : String; grpIndx : Byte; matchPos,
      matchLen : Integer);
    procedure AddToken(const aToken : String; akind : TSqliteTokenKind; matchPos,
      matchLen : Integer);
    procedure AddSpaceIfNeeded;
    function GoNextNonspace(FromPos : Integer) : Integer;
  public
    constructor Create(const aExpr : String); overload;
    constructor Create(aExpr : TSqliteExpr); overload;
    class function FormatSQLExpr(const aExpr : String;
                                  aKwOption : TSqliteKwFormatOption) : String;


    procedure AssignExpr(aExpr : TSqliteExpr); virtual;

    function LastToken : TSqliteToken;
    function LastNSToken : TSqliteToken;

    function IsLiteral: TSqliteToken;
    function IsIdentifier: TSqliteToken;
    function IsNumber: TSqliteToken;
    function IsString: TSqliteToken;
    function IsKeyword: TSqliteToken;
    function IsSingleToken(akinds: TSqliteTokenKinds): TSqliteToken;
    function NextToken(var afrom : integer; akinds : TSqliteTokenKinds
      ) : TSqliteToken;
    function IsEmpty : Boolean;

    property OrigExpr : String read FOrigExpr write FOrigExpr;
    function FormatedStr(aKwOption : TSqliteKwFormatOption) : String;
    function DetectKeywordFormat : TSqliteKwFormatOption;
    function HasClosingSemiColumn : Boolean;
    function Compare(aExpr : TSqliteExpr) : Boolean;

    //constructor methods
    procedure AddK(kw : Word; fmt : TSqliteKwFormatOption); //add keyword
    procedure AddKs(const kw : Array of Word; fmt : TSqliteKwFormatOption); //add keywords
    procedure AddKw(const kw : String); //add not indexed keyword
    procedure AddS(smb : Char); //add symbol
    procedure AddId(const IdStr : String); //add identificator
    procedure AddInt(V : Integer); //add number
    procedure AddReal(V : Double); //add number
    procedure AddStr(const V : String); //add str
    procedure AddExpr(const aExpr : String); overload;
    procedure AddExpr(aExpr : TSqliteExpr); overload;
    procedure AddIdsListed(const Ids : array of String);
    procedure OpenBracket;
    procedure CloseBracket;
    procedure Period;

    property Len : Integer read FLen;
    property Pos : Integer read FPos;

    function TokenAtPos(p : integer) : TSqliteToken;
  end;

  { TSqliteExprs }

  TSqliteExprs = class(specialize TFastBaseCollection<TSqliteExpr>)
  private
    FOrigExpr : String;

    procedure AddExpr(const aExpr : String; matchPos, matchLen : Integer);
  public
    constructor Create(const aExprs : String); overload;
    constructor Create(aExprs : TSqliteExprs); overload;
    class function FormatSQLExprs(const aExprs : String;
                                  aKwOption : TSqliteKwFormatOption;
                                  limit : Integer;
                                  const aSeparator : String = ' ') : String;


    procedure AssignExprs(aExprs : TSqliteExprs); virtual;

    property OrigExpr : String read FOrigExpr write FOrigExpr;
    function FormatedStr(aKwOption : TSqliteKwFormatOption; limit : Integer;
      const aSeparator : String = ' ') : String;
    function DetectKeywordFormat : TSqliteKwFormatOption;

    function ExprAtPos(p : integer) : TSqliteExpr;
  end;

const STOK_BRACKET_OPEN = ord('(');
      STOK_BRACKET_CLOSE = ord(')');
      STOK_PERIOD = ord(',');
      STOK_SEMICOLUMN = ord(';');

implementation

uses LazUTF8;

{ TSqliteExprs }

procedure TSqliteExprs.AddExpr(const aExpr : String; matchPos,
  matchLen : Integer);
var Expr : TSqliteExpr;
begin
  Expr := TSqliteExpr.Create(aExpr);
  Expr.FPos := matchPos;
  Expr.FLen := matchLen;
  Add(Expr);
end;

constructor TSqliteExprs.Create(const aExprs : String);
var
  re : TRegExprWrapper;
  MExpr, CRegExpr : String;
  i : integer;
begin
  inherited Create;

  FOrigExpr:=aExprs;
  MExpr := UTF8Trim(FOrigExpr, [u8tKeepStart]);
  CRegExpr := '(\s*)(((begin(\s*)([^;]+?;)*?(\s*)end)|'+
              '([^;]+?))+)($|;)';
  re := TRegExprWrapper.Create(CRegExpr);
  re.SetModifierR;
  try
    if re.Exec(UTF8LowerCase(MExpr)) then
    begin
      repeat
        if re.SubExprMatchCount >= 2 then
        if re.MatchLen[2] > 0 then
        begin
          AddExpr(Utf8Copy(MExpr, re.MatchPos[2], re.MatchLen[2]),
                                   re.MatchPos[2], re.MatchLen[2]);
        end;
      until not re.ExecNext;
    end;
  finally
    re.Free;
  end;

  i := 0;
  while i < Count do
  begin
    if Self[i].IsEmpty then
    begin
      Delete(i);
    end else
      Inc(i);
  end;
end;

constructor TSqliteExprs.Create(aExprs : TSqliteExprs);
begin
  inherited Create;
  AssignExprs(aExprs);
end;

class function TSqliteExprs.FormatSQLExprs(const aExprs : String;
  aKwOption : TSqliteKwFormatOption; limit : Integer; const aSeparator : String
  ) : String;
var Exprs : TSqliteExprs;
begin
  Exprs := TSqliteExprs.Create(aExprs);
  try
    Result := Exprs.FormatedStr(aKwOption, limit, aSeparator);
  finally
    Exprs.Free;
  end;
end;

procedure TSqliteExprs.AssignExprs(aExprs : TSqliteExprs);
var i : integer;
begin
  FOrigExpr := aExprs.OrigExpr;

  Clear;
  for i := 0 to aExprs.Count-1 do
  begin
    Add(TSqliteExpr.Create(aExprs[i]));
  end;
end;

function TSqliteExprs.FormatedStr(aKwOption : TSqliteKwFormatOption;
  limit : Integer; const aSeparator : String) : String;
var
  i : integer;
begin
  Result := '';
  for i := 0 to Count-1 do
  if (i < limit) or (limit < 0) then
  begin
    Result := Result + Self[i].FormatedStr(aKwOption);
    if not Self[i].HasClosingSemiColumn then
      Result := Result + ';';

    if ((i < (limit - 1)) or (limit < 0)) and (i < (Count - 1)) then
       Result := Result + aSeparator;
  end;
end;

function TSqliteExprs.DetectKeywordFormat : TSqliteKwFormatOption;
var i : integer;
    l : TSqliteKwFormatOption;
    ar : Array [TSqliteKwFormatOption] of SmallInt;
begin
  for l := Low(TSqliteKwFormatOption) to High(TSqliteKwFormatOption) do
    ar[l] := 0;
  for i := 0 to Count-1 do
    Inc(ar[Self[i].DetectKeywordFormat]);

  Result := skfoUpperCase;
  i := 0;
  for l := Low(TSqliteKwFormatOption) to High(TSqliteKwFormatOption) do
  begin
    if (i < ar[l]) then
    begin
      Result := l;
      i := ar[l];
    end;
  end;
end;

function TSqliteExprs.ExprAtPos(p : integer) : TSqliteExpr;
var i : integer;
begin
  for i := 0 to Count-1 do
  begin
    if (Self[i].Pos <= p) and
       ((Self[i].Pos + Self[i].Len) >= p) then
       Exit(Self[i]);
  end;

  Result := nil;
end;

{ TSqliteExpr }

procedure TSqliteExpr.AddTokenStr(const aToken: String; grpIndx : Byte;
                                        matchPos, matchLen : Integer);
var S : String;
begin
  case grpIndx of
    1 :
    begin
      if (sqluCheckKeyWord(UTF8UpperCase(aToken)) > 0) then
        AddToken(aToken, stkKeyWord, matchPos, matchLen)
      else
        AddToken(aToken, stkIdentifier, matchPos, matchLen);
    end;
    2, 3 :
    begin
      AddToken(aToken, stkNumber, matchPos, matchLen);
    end;
    5, 7 : begin
      S := UTF8Copy(aToken, 2, UTF8Length(aToken) - 2);
      S := UTF8StringReplace(S, '''''', '''', [rfReplaceAll]);
      AddToken(S, stkString, matchPos, matchLen);
    end;
    8 : begin
      S := UTF8Copy(aToken, 2, UTF8Length(aToken) - 2);
      S := UTF8StringReplace(S, '""', '"', [rfReplaceAll]);
      AddToken(S, stkIdentifier, matchPos, matchLen);
      if sqluCheckIsNeedQuoted(S) then
        Self[Count-1].FTag[0] := 1;
      end;
    10 : begin
      S := UTF8Copy(aToken, 2, UTF8Length(aToken) - 2);
      S := UTF8StringReplace(S, '``', '`', [rfReplaceAll]);
      AddToken(S, stkIdentifier, matchPos, matchLen);
      if sqluCheckIsNeedQuoted(S) then
        Self[Count-1].FTag[0] := 1;
      end;
    12 : begin
      S := UTF8Copy(aToken, 2, UTF8Length(aToken) - 2);
      AddToken(S, stkIdentifier, matchPos, matchLen);
      if sqluCheckIsNeedQuoted(S) then
        Self[Count-1].FTag[0] := 2;
      end;
    13  :
    begin
      AddToken(aToken, stkSpace, matchPos, matchLen);
    end;
    14  :
    begin
      AddToken(aToken, stkSymbol, matchPos, matchLen);
    end;
  end;
end;

procedure TSqliteExpr.AddToken(const aToken: String; akind: TSqliteTokenKind;
                                        matchPos, matchLen : Integer);
var T : TSqliteToken;
    i : Integer;
begin
  T := TSqliteToken.Create(aToken, aKind);
  if akind = stkKeyWord then
  begin
    i := sqluGetIndexedKeyWordInd(aToken);
    if i >= 0 then
      T.FTag[0] := i else
      T.FTag[0] := 255;
  end else
  if akind = stkSymbol then
  begin
    //\*=\-+;!<>%()\[\]/\.,&|
    T.FTag[0] := Ord(aToken[1]);
  end;
  if aKind <> stkSpace then
    FLastNSToken := T;
  T.FPos := matchPos;
  T.FLen := matchLen;
  Add(T);
end;

procedure TSqliteExpr.AddSpaceIfNeeded;
begin
  if Count > 0 then
  begin
    if Self[Count-1].Kind <> stkSpace then
    begin
      if (Self[Count-1].Kind = stkSymbol) and
         (Self[Count-1].SymbolOrd = STOK_BRACKET_OPEN) then
         Exit;
      AddToken(' ', stkSpace, 0, 0);
    end;
  end;
end;

function TSqliteExpr.GoNextNonspace(FromPos: Integer): Integer;
var ind : integer;
begin
  Result := FromPos;
  for ind := FromPos+1 to count-1 do
  begin
    Inc(Result);
    if Self[Result].Kind <> stkSpace then begin
      Exit;
    end;
  end;
  Result := Count;
end;

constructor TSqliteExpr.Create(const aExpr: String);
var
  re : TRegExprWrapper;
  CRegExpr, MExpr : String;
  i, c, i0, idlen : integer;
begin
  inherited Create;

  FLastNSToken := nil;
  FOrigExpr:=aExpr;
  CRegExpr := '([_a-zA-Z]+[0-9_a-zA-Z]*)|'+
              '(0[xX]\d+)|'+
              '([+\-]{0,1}\d*\.{0,1}\d+([eE][\-+]{0,1}\d+){0,1})|'+
              '(''([^'']|'''')*'')|("")|'+
              '("([^"]|"")+")|'+
              '(`([^`]|``)+`)|'+
              '(\[.+\])|'+
              '(\s+)|([\*=\-+;!<>\$%\(\)\[\]/\.,&\|])';
  re := TRegExprWrapper.Create(CRegExpr);
  re.SetModifierR;
  try
    if re.Exec(FOrigExpr) then
    begin
      repeat
        for i := 1 to re.SubExprMatchCount do
        if re.MatchLen[i] > 0 then
        begin
          AddTokenStr(re.Match[i], i, re.MatchPos[i], re.MatchLen[i]);
        end;
      until not re.ExecNext;
    end;
  finally
    re.Free;
  end;

  //Trim Expression
  while (Count > 0) and (Self[0].Kind = stkSpace) do
    Delete(0);
  for i := Count-1 downto 0 do
  begin
    if Self[i].Kind = stkSpace then
      Delete(i) else
      Break;
  end;

  i := 0;
  while i < Count do
  begin
    if Self[i].Kind = stkIdentifier then
    begin
      idlen := 1;
      i0 := i; c := 0;
      MExpr := Self[i].Token;
      Inc(i);
      While (i < Count) Do begin
        if (Self[i].Kind = stkSymbol) and
           (Self[i].Token = '.') and
           (c = 0) then
        begin
          MExpr := MExpr + '.';
          Inc(i);
          C := 1;
        end
        else
        if (Self[i].Kind = stkIdentifier) and
           (c = 1) then
        begin
          if idlen = 1 then
            Self[i0].AddSubToken(TSqliteToken.Create(Self[i0]));

          inc(idlen);
          MExpr := MExpr + Self[i].Token;
          Self[i0].FToken := MExpr;
          Self[i0].AddSubToken(Self[i]);
          Self[i] := nil; DeleteFreeAssigned(i);
          Dec(i);
          Delete(i);
          C := 0;
        end else Break;
      end;
      Self[i0].FTag[1] := idlen;
    end else Inc(i);
  end;
end;

constructor TSqliteExpr.Create(aExpr : TSqliteExpr);
begin
  inherited Create;
  AssignExpr(aExpr);
end;

class function TSqliteExpr.FormatSQLExpr(const aExpr : String;
  aKwOption : TSqliteKwFormatOption) : String;
var Expr : TSqliteExpr;
begin
  Expr := TSqliteExpr.Create(UTF8Trim(aExpr));
  try
    Result := Expr.FormatedStr(aKwOption);
  finally
    Expr.Free;
  end;
end;

procedure TSqliteExpr.AssignExpr(aExpr : TSqliteExpr);
var i : integer;
begin
  FOrigExpr := aExpr.OrigExpr;

  Clear;
  for i := 0 to aExpr.Count-1 do
  begin
    Add(TSqliteToken.Create(aExpr[i]));
  end;
end;

function TSqliteExpr.LastToken: TSqliteToken;
begin
  if Count > 0 then Result := Self[count-1] else
                    Result := nil;
end;

function TSqliteExpr.LastNSToken: TSqliteToken;
begin
  Result := FLastNSToken;
end;

function TSqliteExpr.IsLiteral: TSqliteToken;
begin
  Result := IsSingleToken([stkNumber, stkString]);
  if not Assigned(Result) then
  begin
    Result := IsSingleToken([stkKeyWord]);
    if Assigned(Result) then
    begin
      if not (Result.Tag[0] in [kwCURRENT_DATE, kwCURRENT_TIME,
                           kwCURRENT_TIMESTAMP, kwTRUE, kwFALSE, kwNULL]) then
        Exit(nil);
    end;
  end;
end;

function TSqliteExpr.IsIdentifier: TSqliteToken;
begin
  Result := IsSingleToken([stkIdentifier]);
end;

function TSqliteExpr.IsNumber: TSqliteToken;
begin
  Result := IsSingleToken([stkNumber]);
end;

function TSqliteExpr.IsString: TSqliteToken;
begin
  Result := IsSingleToken([stkString]);
end;

function TSqliteExpr.IsKeyword: TSqliteToken;
begin
  Result := IsSingleToken([stkKeyWord]);
end;

function TSqliteExpr.IsSingleToken(akinds: TSqliteTokenKinds): TSqliteToken;
var i : integer;
begin
  i := GoNextNonspace(-1);
  if i < Count then
  begin
    if Self[i].Kind in akinds then
    begin
      if GoNextNonspace(i) = Count then
        Result := Self[i] else
        Result := nil;
    end else Result := nil;
  end else Result := nil;
end;

function TSqliteExpr.NextToken(var afrom : integer; akinds : TSqliteTokenKinds
  ) : TSqliteToken;
begin
  while afrom < Count do
  begin
    if Self[afrom].Kind in akinds then
    begin
      Result := Self[afrom];
      Exit;
    end;
    Inc(afrom);
  end;
  Result := nil;
end;

function TSqliteExpr.IsEmpty : Boolean;
var i : integer;
begin
  for i := 0 to Count-1 do
  if not (Self[i].Kind in [stkSpace, stkSymbol]) then
  begin
    Exit(false);
  end;
  Result := true;
end;

function TSqliteExpr.FormatedStr(aKwOption : TSqliteKwFormatOption) : String;
var i : integer;
begin
  Result := '';
  for i := 0 to Count-1 do
  begin
    case Self[i].Kind of
      stkSpace : Result := Result + ' ';
      stkKeyWord : Result := Result +
                                   sqluFormatKeyWord(Self[i].Token, aKwOption);
    else
      Result := Result + Self[i].QuotedToken;
    end;
  end;
end;

function TSqliteExpr.DetectKeywordFormat : TSqliteKwFormatOption;
var i : integer;
    l : TSqliteKwFormatOption;
    ar : Array [TSqliteKwFormatOption] of SmallInt;
begin
  for l := Low(TSqliteKwFormatOption) to High(TSqliteKwFormatOption) do
    ar[l] := 0;
  for i := 0 to Count-1 do
  if Self[i].Kind = stkKeyWord then
    Inc(ar[sqluDetectFormat(Self[i].Token)]);

  Result := skfoUpperCase;
  i := 0;
  for l := Low(TSqliteKwFormatOption) to High(TSqliteKwFormatOption) do
  begin
    if (i < ar[l]) then
    begin
      Result := l;
      i := ar[l];
    end;
  end;
end;

function TSqliteExpr.HasClosingSemiColumn : Boolean;
var i : integer;
begin
  for i := Count-1 downto 0 do
  begin
    if Self[i].Kind = stkSpace then
    begin
      Continue;
    end else
    if (Self[i].Kind = stkSymbol) and
       (self[i].SymbolOrd = STOK_SEMICOLUMN) then
    begin
      Exit(True);
    end else
      Exit(False);
  end;
end;

function TSqliteExpr.Compare(aExpr : TSqliteExpr) : Boolean;
var k1, k2 : integer;
begin
  if not Assigned(aExpr) then Exit(False);
  k1 := 0;
  k2 := 0;
  while (k1 < Count) do
  begin
    if (Self[k1].Kind = stkSpace) then
    begin
      inc(k1);
      Continue;
    end;

    while (k2 < aExpr.Count) do
    begin
      if (aExpr[k2].Kind = stkSpace) then
        Inc(k2) else
        Break;
    end;

    if (k1 < Count) and (k2 < aExpr.Count) then
    begin
      if not Self[k1].Compare(aExpr[k2]) then
        Exit(false);
    end else
    if (k2 = aExpr.Count) then
    begin
      Exit(false);
    end;

    Inc(k1); Inc(k2);
  end;
  while (k2 < aExpr.Count) do
  begin
    if (aExpr[k2].Kind = stkSpace) then
      Inc(k2) else
      Exit(false);
  end;
  Result := true;
end;

procedure TSqliteExpr.AddK(kw : Word; fmt : TSqliteKwFormatOption);
var T : TSqliteToken;
begin
  AddSpaceIfNeeded;
  T := TSqliteToken.Create(sqluGetIndexedKeyWord(kw, fmt), stkKeyWord);
  T.FTag[0] := kw;
  FLastNSToken := T;
  Add(T);
end;

procedure TSqliteExpr.AddKs(const kw : array of Word;
  fmt : TSqliteKwFormatOption);
var T : TSqliteToken;
    i : integer;
begin
  for i := Low(kw) to High(kw) do
  begin
    AddSpaceIfNeeded;
    T := TSqliteToken.Create(sqluGetIndexedKeyWord(kw[i], fmt), stkKeyWord);
    T.FTag[0] := kw[i];
    FLastNSToken := T;
    Add(T);
  end;
end;

procedure TSqliteExpr.AddKw(const kw : String);
begin
  AddSpaceIfNeeded;
  AddToken(kw, stkKeyWord, 0, 0);
end;

procedure TSqliteExpr.AddS(smb : Char);
var T : TSqliteToken;
begin
  if not (smb in [',', ')']) then
    AddSpaceIfNeeded;
  T := TSqliteToken.Create(smb, stkSymbol);
  T.FTag[0] := Ord(smb);
  FLastNSToken := T;
  Add(T);
end;

procedure TSqliteExpr.AddId(const IdStr : String);
var T : TSqliteToken;
begin
  AddSpaceIfNeeded;
  T := TSqliteToken.Create(IdStr, stkIdentifier);
  T.FTag[1] := 1;
  if sqluCheckIsNeedQuoted(IdStr) then
    T.FTag[0] := 8;
  FLastNSToken := T;
  Add(T);
end;

procedure TSqliteExpr.AddInt(V : Integer);
begin
  AddSpaceIfNeeded;
  AddToken(Inttostr(V), stkNumber, 0, 0);
end;

procedure TSqliteExpr.AddReal(V : Double);
begin
  AddSpaceIfNeeded;
  AddToken(FloatToStr(V), stkNumber, 0, 0);
end;

procedure TSqliteExpr.AddStr(const V : String);
begin
  AddSpaceIfNeeded;
  AddToken(V, stkString, 0, 0);
end;

procedure TSqliteExpr.AddExpr(const aExpr : String);
var Expr : TSqliteExpr;
begin
  Expr := TSqliteExpr.Create(aExpr);
  try
    AddExpr(Expr);
  finally
    Expr.Free;
  end;
end;

procedure TSqliteExpr.AddExpr(aExpr : TSqliteExpr);
var i : integer;
    T : TSqliteToken;
begin
  if aExpr.Count > 0 then
  begin
    AddSpaceIfNeeded;
    for i := 0 to aExpr.Count-1 do
    begin
      T := TSqliteToken.Create(aExpr[i]);
      Add(T);
    end;
  end;
end;

procedure TSqliteExpr.AddIdsListed(const Ids : array of String);
var i : integer;
begin
  for i := Low(Ids) to High(Ids) do
  begin
    if i > Low(ids) then Period;
    AddId(Ids[i]);
  end;
end;

procedure TSqliteExpr.OpenBracket;
begin
  AddS('(');
end;

procedure TSqliteExpr.CloseBracket;
begin
  AddS(')');
end;

procedure TSqliteExpr.Period;
begin
  AddS(',');
end;

function TSqliteExpr.TokenAtPos(p : integer) : TSqliteToken;
var i, j : integer;
begin
  for i := 0 to Count-1 do
  begin
    if Self[i].SubTokenCnt > 0 then
    begin
      for j := 0 to Self[i].SubTokenCnt-1 do
      begin
        if (Self[i].SubToken[j].Pos <= p) and
           ((Self[i].SubToken[j].Pos + Self[i].SubToken[j].Len) >= p) then
           Exit(Self[i]);
      end;
    end else
    if (Self[i].Pos <= p) and
       ((Self[i].Pos + Self[i].Len) >= p) then
       Exit(Self[i]);
  end;
  Result := nil;
end;

{ TSqliteToken }

function TSqliteToken.GetIsQuoted : Boolean;
begin
  if Kind = stkIdentifier then
  begin
    Result := (Tag[0] > 0);
  end else
  if Kind = stkString then
  begin
    Result := true;
  end;
end;

function TSqliteToken.GetQuotedToken : String;
begin
  case Kind of
    stkIdentifier:
    begin
      if (Tag[0] > 0) then
        Result := '"' + UTF8StringReplace(Token, '"', '""', [rfReplaceAll]) +  '"' else
        Result := Token;
    end;
    stkString:
      Result := '''' + UTF8StringReplace(Token, '''', '''''', [rfReplaceAll]) +  '''';
    stkKeyWord:
      Result := UpperCase(Token);
  else
    Result := Token;
  end;
end;

function TSqliteToken.GetSubToken(Index : Integer) : TSqliteToken;
begin
  Result := FSubTokens[index];
end;

function TSqliteToken.GetSubTokenCnt: Integer;
begin
  if Assigned(FSubTokens) then Result := FSubTokens.Count else
                               Result := 0;
end;

function TSqliteToken.GetSubTokenStr(Index : Integer) : String;
begin
  Result := FSubTokens[index].Token;
end;

function TSqliteToken.GetTag(Index : Integer) : Byte;
begin
  Result := FTag[Index];
end;

constructor TSqliteToken.Create(const aToken: String; aKind: TSqliteTokenKind);
begin
  FSubTokens := nil;
  Ftoken := aToken;
  FKind := aKind;
  FillChar(FTag, Length(FTag), 0);
end;

constructor TSqliteToken.Create(aToken: TSqliteToken);
begin
  FSubTokens := nil;
  AssignToken(aToken);
end;

procedure TSqliteToken.AssignToken(aToken : TSqliteToken);
var i : integer;
begin
  Ftoken := aToken.Token;
  FKind := aToken.Kind;
  Move(aToken.FTag, FTag, Length(FTag));
  if Assigned(aToken.FSubTokens) then
  begin
    if Assigned(FSubTokens) then FSubTokens.Clear else
                                 FSubTokens := TSqliteTokens.Create;
    for i := 0 to aToken.FSubTokens.Count-1 do
    begin
      FSubTokens.Add(TSqliteToken.Create(aToken.FSubTokens[i]));
    end;
  end else
    if Assigned(FSubTokens) then FreeAndNil(FSubTokens);
end;

function TSqliteToken.Compare(aToken : TSqliteToken) : Boolean;
begin
  if Kind = aToken.Kind then
  begin
    case Kind of
      stkIdentifier, stkKeyWord, stkFieldName, stkTableName,
      stkDataType :
        Result := sqluCompareNames(Token, aToken.Token);
      stkSymbol, stkString, stkNumber : Result := SameText(Token, Token);
    else
      Result := true;
    end;
  end else
    Result := false;
end;

function TSqliteToken.KeyWordIndx : Byte;
begin
  Result := FTag[0];
end;

function TSqliteToken.SymbolOrd : Byte;
begin
  Result := FTag[0];
end;

function TSqliteToken.IdCnt : Byte;
begin
  Result := FTag[1];
end;

function TSqliteToken.TagInt : Int32;
begin
  Result := PInt32(@(FTag[0]))^;
end;

destructor TSqliteToken.Destroy;
begin
  if Assigned(FSubTokens) then FSubTokens.Free;
  inherited Destroy;
end;

procedure TSqliteToken.AddSubToken(sT: TSqliteToken);
begin
  if not Assigned(FSubTokens) then FSubTokens := TSqliteTokens.Create;
  FSubTokens.Add(sT);
end;


end.


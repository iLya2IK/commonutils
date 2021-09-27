{
 ExprComparator:
   Classes for expression processing

   Copyright (c) 2021 by Ilya Medvedkov

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
}

unit ExprComparator;

{$mode objfpc}{$H+}
{.$define regexpr_new}
{$define regexpr_t}
{$ifdef regexpr_new}{$define regexpr_nt}{$endif}
{$ifdef regexpr_t}{$define regexpr_nt}{$endif}

interface

uses
  Classes, SysUtils, OGLFastList,
  {$ifdef regexpr_new}
  RegExpr
  {$else}
  {$ifdef regexpr_t}
  regexpr_t
  {$else}
  uregexpr
  {$endif}
  {$endif};

type

FloatArray  = array[0..$efff] of Single;
TFloatArray = FloatArray;
PFloatArray = ^FloatArray;

TSinTokenKind = Byte;

TSinExprSep = (sesNone, sesDot, sesPeriod, sesDotPeriod);
TSinExprSeps = set of TSinExprSep;

{ TSinToken }

TSinToken = class
private
  FToken, FCyrToken : String;
  FUTF8TokenLength, FUTF8CyrTokenLength : Integer;
  FKind : TSinTokenKind;
public
  class function CheckIsLat(const S : String) : Word;
  class function CheckIsNumeric(const S : String) : Word;
  class function CheckIsSeparator(const S : String) : TSinExprSep;
  class function DLdist(const afirst, asecond: String; U8lFirst,
    U8lSecond: Integer): Single;
  class function LatToCyr(const S: String; U8len: integer): String;

  constructor Create(const aToken : String);
  function Compare(aE : TSinToken) : Single;
  property Token : String read FToken;
  property CyrToken : String read FCyrToken;
  property UTF8TokenLength : Integer read FUTF8TokenLength;
  property UTF8CyrTokenLength : Integer read FUTF8CyrTokenLength;
  property Kind : TSinTokenKind read FKind;
end;

{ TSinExpr }

TSinExpr = class(TFastCollection)
private
  FOrigExpr : String;
  function GetToken(index : integer): TSinToken;
  class function DLdist(const afirst, asecond: TSinExpr) : Single;
public
  constructor Create(const aExpr : String; AllowedSeparators : TSinExprSeps = []); overload;
  constructor Create(Expr1, Expr2 : TSinExpr; AllowedSeparators : TSinExprSeps = []); overload;
  property Token[index : integer] : TSinToken read GetToken; default;
  function Compare(aE : TSinExpr) : Single;
  function Contain(aE : TSinExpr) : Single;
  function CrossHitRate(aE : TSinExpr) : Single;
  function CrossHitContain(aE: TSinExpr; minWordLen : integer = -1): Single;

  property OrigExpr : String read FOrigExpr write FOrigExpr;
end;

{ TSinExprList }

TSinExprList = class(TFastCollection)
private
  FConcatedExprs : TFastCollection;
  FSinExpr : TSinExpr;
  function GetConcatExpr(index : integer): TSinExpr;
  function GetExpr(index : integer): TSinExprList;
public
  constructor Create(const aExpr : String); overload;
  destructor Destroy; override;
  procedure ConcatinateAll;
  property SelfSinExpr : TSinExpr read FSinExpr;
  property Expr[index : integer] : TSinExprList read GetExpr; default;
  property ConcatExpr[index : integer] : TSinExpr read GetConcatExpr;
end;

function SinRegExpr(const aInp, aExpr : String) : Boolean;
function SinRegExprVersionMajor : Integer;
function SinRegExprVersionMinor : Integer;

const
  sktRusString = byte($01);
  sktEnString  = byte($02);
  sktNumber    = byte($04);
  sktSeparator = byte($08);
  sktNumberOrSeparator = sktNumber or sktSeparator;

implementation

uses Math, LazUTf8;

function SinRegExpr(const aInp, aExpr : String) : Boolean;
var re : TRegExpr;
begin
  re := TRegExpr.Create(UnicodeString(aExpr));
  re.ModifierR := true;
  try
    Result := re.Exec(UnicodeString(aInp));
  finally
    re.Free;
  end;
end;

function SinRegExprVersionMajor : Integer;
begin
  Result := TRegExpr.VersionMajor;
end;

function SinRegExprVersionMinor : Integer;
begin
  Result := TRegExpr.VersionMinor;
end;

{ TSinExprList }

function TSinExprList.GetConcatExpr(index : integer): TSinExpr;
begin
  if index < FConcatedExprs.Count then
  begin
    if Assigned(FConcatedExprs[index]) then
    begin
      Result := TSinExpr(FConcatedExprs[index]);
      Exit;
    end;
  end else
    FConcatedExprs.Count := index + 1;
  FConcatedExprs[index] := TSinExpr.Create(FSinExpr, GetExpr(index).SelfSinExpr);
  Result :=TSinExpr(FConcatedExprs[index]);
end;

function TSinExprList.GetExpr(index: integer): TSinExprList;
begin
  Result := TSinExprList(Item[index]);
end;

constructor TSinExprList.Create(const aExpr: String);
begin
  Inherited Create;
  FSinExpr := TSinExpr.Create(aExpr);
  FConcatedExprs := TFastCollection.Create;
end;

destructor TSinExprList.Destroy;
begin
  FSinExpr.Free;
  FConcatedExprs.Free;
  inherited Destroy;
end;

procedure TSinExprList.ConcatinateAll;
var i : integer;
    L : TSinExprList;
begin
  for i := 0 to Count-1 do
  begin
    GetConcatExpr(i);
    L := GetExpr(i);
    L.ConcatinateAll;
  end;
end;

{ TSinExpr }

function TSinExpr.GetToken(index: integer): TSinToken;
begin
  Result := TSinToken(Item[index]);
end;

class function TSinExpr.DLdist(const afirst, asecond: TSinExpr): Single;
var firstLength, secondLength, amax, i, afrom, ato, j : integer;
    value, cost : Single;
    secondCh, firstCh : TSinToken;
    currentRow, previousRow, transpositionRow, tempRow, data : PFloatArray;
    currentRowLen : Integer;
begin
    firstLength := afirst.Count;
    secondLength := asecond.Count;

    if (firstLength = 0) or (secondLength = 0) then Exit(1.0);

    if (firstLength > secondLength) then
       Exit(DLDist(asecond, afirst));

    amax := secondLength;
    if (secondLength - firstLength > amax) then Exit(1.0);

    currentRowLen := (firstLength + 1) ;

    data := AllocMem(currentRowLen * 3 * sizeOf(single));
    currentRow := @(data^[0]);
    previousRow := @(data^[currentRowLen]);
    transpositionRow := @(data^[currentRowLen * 2]);

    for i := 0 to firstLength do
      previousRow^[i] := i;

    for i := 1 to secondLength do begin
      secondCh := asecond[i-1];
      currentRow^[0] := i;

      afrom := Math.max(i - amax - 1, 1);
      ato := Math.min(i + amax + 1, firstLength);

      for j := afrom to ato do begin
          firstCh := afirst[j-1];

          cost  := Math.min(firstCh.Compare(secondCh), 1.0);

          value := Math.min(Math.min(currentRow^[j - 1] + 1.0,
                                                   previousRow^[j] + 1.0),
                                                   previousRow^[j - 1] + cost);

          currentRow^[j] := value;
      end;

      tempRow := transpositionRow;
      transpositionRow := previousRow;
      previousRow := currentRow;
      currentRow := tempRow;
    end;
    Result := Math.min(previousRow^[firstLength] /
                                      (firstLength + secondLength) * 2.0, 1.0);
    FreeMem(data);
end;

constructor TSinExpr.Create(Expr1, Expr2: TSinExpr;
  AllowedSeparators: TSinExprSeps);
begin
  Create(Expr1.OrigExpr + ' ' + Expr2.OrigExpr, AllowedSeparators);
end;

constructor TSinExpr.Create(const aExpr: String; AllowedSeparators: TSinExprSeps
  );
var
    re : TRegExpr;
    MExpr, CRegExpr : String;
    T : TSinToken;
begin
  inherited Create;
  FOrigExpr:=aExpr;
  MExpr := UTF8LowerCase(Utf8Trim(FOrigExpr));
  CRegExpr := {$ifdef regexpr_nt}'(([а-я]+)'{$else}'(([а-пр-я]+)'{$endif}+
              '|([a-z]+)|(\d+)';
  if AllowedSeparators <> [] then
  begin
    if sesPeriod in AllowedSeparators then
      CRegExpr:= CRegExpr + '|(,)';
    if sesDotPeriod in AllowedSeparators then
      CRegExpr:= CRegExpr + '|(\;)';
    if sesDot in AllowedSeparators then
      CRegExpr:= CRegExpr + '|(\.)';
  end;
  CRegExpr := CRegExpr + ')';
  re := TRegExpr.Create(UnicodeString(CRegExpr));
  re.ModifierR := true;
  try
    if re.Exec(UnicodeString(MExpr)) then
    begin
      repeat
        T := TSinToken.Create(UTF8Encode(re.Match[0]));
        Add(T);
      until not re.ExecNext;
    end;
  finally
    re.Free;
  end;
end;

function TSinExpr.Compare(aE: TSinExpr): Single;
begin
  Result := DLdist(Self, aE);
end;

function TSinExpr.Contain(aE: TSinExpr): Single;
var i, j, k : integer;
    v : Single;
begin
  if aE.Count = 0 then Exit(1);

  Result := 1;
  i := 0;
  while i <= (Count - aE.Count) do
  begin
    v := 0;
    k := 0;
    for j := i to (i + aE.Count - 1) do
    begin
      v := Math.Max(Token[i].Compare(aE.Token[k]), v);
      inc(k);
    end;
    if Result > v then Result := v;
    Inc(i);
  end;
end;

function TSinExpr.CrossHitRate(aE: TSinExpr): Single;
var i, j : integer;
begin
  if (Count = 0) or (aE.Count = 0) then Exit(1);

  Result := 0;
  for i := 0 to Count-1 do
  begin
    for j := 0 to aE.Count-1 do
      Result := Result + Token[i].Compare(aE.Token[j]);
  end;
  Result := Result / (Count * aE.Count);
end;

function TSinExpr.CrossHitContain(aE: TSinExpr; minWordLen: integer): Single;
var i, j, k, C : integer;
    bv, v : Single;
    Excludes : Array of Boolean;
begin
  if (Count = 0) or (aE.Count = 0) then Exit(1);

  SetLength(Excludes, aE.Count);
  FillByte(Excludes[0], aE.Count, 0);

  C := 0;
  Result := 0;
  for i := 0 to Count-1 do
  if (minWordLen < Token[i].UTF8TokenLength) then
  begin
    bv := 1.0;
    k := -1;
    for j := 0 to aE.Count-1 do
    if not Excludes[j] then
    if (minWordLen < aE.Token[j].UTF8TokenLength) then
    begin
      v := Token[i].Compare(aE.Token[j]);
      if v < bv then begin
        bv := v;
        k := j;
      end;
    end;
    if k >= 0 then Excludes[k] := true;
    Result := Result + bv;
    inc(C);
  end;
  if C = 0 then Exit(1);
  Result := Result / single(C);
end;

{ TSinToken }

constructor TSinToken.Create(const aToken: String);
begin
  FToken := aToken;
  if CheckIsSeparator(FToken) <> sesNone then
     FKind:= sktSeparator else
  if CheckIsNumeric(FToken) = 1 then
     FKind:= sktNumber else
  begin
    if CheckIsLat(FToken) = 1 then
       FKind:= sktEnString else
       FKind:= sktRusString;
  end;
  FUTF8TokenLength := UTF8Length(FToken);
  if Kind = sktEnString then begin
    FCyrToken := LatToCyr(FToken, FUTF8TokenLength);
    FUTF8CyrTokenLength := UTF8Length(FCyrToken);
  end else begin
    FCyrToken := FToken;
    FUTF8CyrTokenLength := FUTF8TokenLength;
  end;
end;

function TSinToken.Compare(aE: TSinToken): Single;
begin
  if not Assigned(aE) then Exit(1.0);
  if (Kind <> aE.Kind) then
  begin
    if (((Kind or aE.Kind) and sktNumberOrSeparator) > 0) then
        Result := 1 else
        Result := DLdist(FCyrToken, aE.CyrToken,
                         FUTF8CyrTokenLength, aE.UTF8CyrTokenLength);
  end else
     if (Kind = sktSeparator) then
       Result := 0
     else
       Result := DLdist(FToken, aE.Token,
                        FUTF8TokenLength, aE.UTF8TokenLength);
end;

class function TSinToken.LatToCyr(const S: String; U8len : integer): String;
var i : Integer;
  ch, res, resloc : PChar;
  passed: Boolean;

procedure AddToRes(const toadd : String);
var rl : Integer;
begin
  rl := length(toadd);
  Move(toadd[1], resloc^, rl);
  Inc(resloc, rl);
end;

procedure IncCh; inline;
begin
  Inc(i);
  Inc(ch);
end;

procedure DecCh; inline;
begin
  Dec(i);
  Dec(ch);
end;

begin
  if U8len > 0 then
  begin
    if U8len <> Length(S) then Exit(S);

    res := GetMem(U8len shl 2);
    try
      resloc := res;
      ch := PChar(@(S[1]));
      i := 0;
      while (i < U8len) do
      begin
        passed := true;

        if ((i + 1) <  U8len) then
        begin
          if (ch^ = 'j') then // Префиксная нотация вначале
          begin
            passed := false;
            IncCh;
            if ch^ = 'e' then AddToRes('ё') else
            if ch^ = 's' then AddToRes('щ') else
            if ch^ = 'h' then AddToRes('ь') else
            if ch^ = 'u' then AddToRes('ю') else
            if ch^ = 'a' then AddToRes('я') else begin
              DecCh;
              passed := true;
            end;
          end else
          begin
            IncCh;
            if ch^ = 'h' then
            begin
              DecCh;
              passed := false;
	      if (ch^ = 'z') then AddToRes('ж') else
	      if (ch^ = 'k') then AddToRes('х') else
	      if (ch^ = 'c') then AddToRes('ч') else
	      if (ch^ = 's') then AddToRes('ш') else
	      if (ch^ = 'e') then AddToRes('э') else
	      if (ch^ = 'h') then AddToRes('ъ') else
	      if (ch^ = 'i') then AddToRes('ы') else
                passed := true;
              if not passed then  IncCh;
            end else
              DecCh;
          end;
        end;
        if passed then
        begin // одиночные символы
          if (ch^ = 'a') then AddToRes('а') else
          if (ch^ = 'b') then AddToRes('б') else
          if (ch^ = 'v') then AddToRes('в') else
          if (ch^ = 'w') then AddToRes('в') else
          if (ch^ = 'g') then AddToRes('г') else
          if (ch^ = 'd') then AddToRes('д') else
          if (ch^ = 'e') then AddToRes('е') else
          if (ch^ = 'z') then AddToRes('з') else
          if (ch^ = 'i') then AddToRes('и') else
          if (ch^ = 'y') then AddToRes('й') else
          if (ch^ = 'k') then AddToRes('к') else
          if (ch^ = 'l') then AddToRes('л') else
          if (ch^ = 'm') then AddToRes('м') else
          if (ch^ = 'n') then AddToRes('н') else
          if (ch^ = 'o') then AddToRes('о') else
          if (ch^ = 'p') then AddToRes('п') else
          if (ch^ = 'r') then AddToRes('р') else
          if (ch^ = 's') then AddToRes('с') else
          if (ch^ = 't') then AddToRes('т') else
          if (ch^ = 'u') then AddToRes('у') else
          if (ch^ = 'f') then AddToRes('ф') else
          if (ch^ = 'c') then AddToRes('ц') else
          if (ch^ = 'x') then AddToRes('кс') else
          if (ch^ = 'q') then AddToRes('ку') else
          if (ch^ = 'j') then AddToRes('дж') else
          if (ch^ = 'h') then AddToRes('х') else
          begin
            Move(ch^, resloc^, 1); // numbers
            Inc(resloc);
          end;
        end;
        IncCh; // переходим к следующему символу
      end;
      resloc^ := #0;
      Result := StrPas(res);
    finally
      FreeMem(res);
    end;
  end else Result := '';
end;

class function TSinToken.DLdist(const afirst, asecond: String;
                                      U8lFirst, U8lSecond : Integer): Single;

const cLatToPosMat : Array ['a'..'z'] of TPoint =
 {a-e:} ((x:0;y:1), (x:4;y:2), (x:2;y:2), (x:2;y:1), (x:2;y:0),
 {f-j:}  (x:3;y:1), (x:4;y:1), (x:5;y:1), (x:7;y:0), (x:6;y:1),
 {k-o:}  (x:7;y:1), (x:8;y:1), (x:6;y:2), (x:5;y:2), (x:8;y:0),
 {p-u:}  (x:9;y:0), (x:0;y:0), (x:3;y:0), (x:1;y:1), (x:4;y:0), (x:6;y:0),
 {v-z:}  (x:3;y:2), (x:1;y:0), (x:1;y:2), (x:5;y:0), (x:0;y:2));

const cCyrToPosMat1 : Array [$b0..$bf] of TPoint = // $d0 prefix
 {а-е:} ((x:3;y:1), (x:7;y:2), (x:2;y:1), (x:6;y:0), (x:8;y:1), (x:4;y:0),
 {ж-л:}  (x:9;y:1), (x:9;y:0), (x:4;y:2), (x:0;y:0), (x:3;y:0), (x:7;y:1),
 {м-п:}  (x:3;y:2), (x:5;y:0), (x:6;y:1), (x:4;y:1));

const cCyrToPosMat2 : Array [$80..$8f] of TPoint = // $d1 prefix
 {р-т:} ((x:5;y:1), (x:2;y:2), (x:5;y:2),
 {у-ш:}  (x:2;y:0), (x:0;y:1), (x:10;y:0),(x:1;y:0), (x:1;y:2), (x:7;y:0),
 {щ-ю:}  (x:8;y:0), (x:11;y:0),(x:1;y:1),(x:6;y:2),(x:10;y:1),(x:8;y:2),
 {я:}    (x:0;y:2) );

function CharPos(C : PChar) : TPoint;
var U8Len : integer;
begin
  U8Len := UTF8CodepointSizeFast(C);
  case U8Len of
    1: begin
      if C^ in ['1'..'9'] then
         Result := Point(Ord(C^) - Ord('1'),10) else
      if C^='0' then
         Result := Point(9,10) else
      if C^ in ['a'..'z'] then
         Result := cLatToPosMat[C^]
      else
         Result := Point(-10, -10);
    end;
    2: begin
      if Ord(C^) = $d0 then
      begin
        Inc(C);
        if Ord(C^) in [$b0..$bf] then
          Result := cCyrToPosMat1[Ord(C^)] else
          Result := Point(-10,-10);
      end else
      if Ord(C^) = $d1 then
      begin
        Inc(C);
        if Ord(C^) in [$80..$8f] then
          Result := cCyrToPosMat2[Ord(C^)] else
          Result := Point(-10,-10);
      end else
        Result := Point(-10,-10);
    end;
    else
       Result := Point(-10,-10);
  end;
end;

function charGaussDistance(C1, C2 : PChar) : Single;
var P1, P2 : TPoint;
begin
  P1 := CharPos(C1);
  P2 := CharPos(C2);
  Result := Math.max(abs(p1.x - p2.x), abs(p1.y - p2.y)) * 0.5;
end;

function U8Compare(C1, C2 : PChar) : Boolean;
var l1, l2 : Integer;
begin
  if Assigned(C1) and Assigned(C2) then
  begin
    l1 := UTF8CodepointSizeFast(C1);
    l2 := UTF8CodepointSizeFast(C2);
    if (l1 = l2) then
    begin
      while l1 > 0 do
      begin
        if (C1^ <> C2^) then Exit(false);
        Inc(C1); Inc(C2); Dec(l1);
      end;
      Result := true;
    end else Result := false;
  end else Result := false;
end;

var amax, i, afrom, ato, j,
    fLen : integer;
    value, cost : Single;
    lastSecondCh, secondCh, lastFirstCh, firstCh : PChar;
    currentRow, previousRow, transpositionRow, tempRow, data : PFloatArray;
    currentRowLen : Integer;
begin
    if (U8lFirst = 0) or (U8lSecond = 0) then Exit(1.0);

    if (U8lFirst > U8lSecond) then
       Exit(DLDist(asecond, afirst, U8lSecond, U8lFirst));

    fLen := Length(afirst);

    amax := U8lSecond;
    if (U8lSecond - U8lFirst > amax) then Exit(1.0);

    currentRowLen := (U8lFirst + 1) ;

    data := AllocMem(currentRowLen * 3 * sizeOf(single));
    currentRow := @(data^[0]);
    previousRow := @(data^[currentRowLen]);
    transpositionRow := @(data^[currentRowLen * 2]);

    for i := 0 to U8lFirst do
      previousRow^[i] := i;

    lastSecondCh := nil;
    secondCh := PChar(@(asecond[1]));
    for i := 1 to U8lSecond do begin
       currentRow^[0] := i;

       // Вычисляем только диагональную полосу шириной 2 * (max + 1)
       afrom := Math.max(i - amax - 1, 1);
       ato := Math.min(i + amax + 1, U8lFirst);

       lastFirstCh := nil;
       firstCh := UTF8CodepointStart(PChar(@(afirst[1])), fLen, afrom-1);
       for j := afrom to ato do begin
         // Вычисляем минимальную цену перехода в текущее состояние из
         // предыдущих среди удаления, вставки и замены соответственно.
         if U8Compare(firstCh, secondCh) then
           cost := 0 else
           cost := min(charGaussDistance(firstCh, secondCh),1.0);

         value := Math.min(Math.min(currentRow^[j - 1] + 1.0,
                                    previousRow^[j] + 1.0),
                                    previousRow^[j - 1] + cost);

         // Если вдруг была транспозиция, надо также учесть и её стоимость.
         if (i > 1) and (j > 1) then
           if (U8Compare(firstCh, lastSecondCh) and
               U8Compare(lastFirstCh, secondCh)) then
       	      value := Math.min(value, transpositionRow^[j - 2] + 0.5);

         currentRow^[j] := value;
         lastFirstCh := firstCh;
         inc(firstCh, UTF8CodepointSizeFast(firstCh));
       end;
       lastSecondCh := secondCh;
       inc(secondCh, UTF8CodepointSizeFast(secondCh));

       tempRow := transpositionRow;
       transpositionRow := previousRow;
       previousRow := currentRow;
       currentRow := tempRow;
    end;
    Result := Math.min(previousRow^[U8lFirst] /
                                       (U8lFirst + U8lSecond) * 2.0, 1.0);
    FreeMem(data);
end;

class function TSinToken.CheckIsSeparator(const S: String): TSinExprSep;
var C : PChar;
begin
  if Length(S) = 1 then
  begin
    C := PChar(@(S[1]));
    if (C^ = ';') then
       Result := sesDotPeriod else
    if (C^ = '.') then
       Result := sesDot else
    if (C^ = ',') then
       Result := sesPeriod  else
       Result := sesNone;
  end else Result := sesNone;
end;

class function TSinToken.CheckIsNumeric(const S: String): Word;
begin
  if Length(S) > 0 then
  begin
    if UTF8CodepointSizeFast(PChar(@(S[1]))) = 1 then
    begin
      if PChar(@(S[1]))^ in ['0'..'9'] then
        Result := 1 else
        Result := 2;
    end else Result := 2;
  end else Result := 0;
end;

class function TSinToken.CheckIsLat(const S: String): Word;
begin
  if Length(S) > 0 then
  begin
    if UTF8CodepointSizeFast(PChar(@(S[1]))) = 1 then
    begin
      if PChar(@(S[1]))^ in ['a'..'z'] then
        Result := 1 else
        Result := 2;
    end else Result := 2;
  end else Result := 0;
end;

end.


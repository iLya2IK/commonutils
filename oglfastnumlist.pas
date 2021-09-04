{ OGLFastNumList
   lightweighted numeric lists
   Copyright (c) 2021 Ilya Medvedkov   }

unit OGLFastNumList;

{$mode objfpc}
{$ifdef CPUX86_64}
{$asmMode intel}
{$endif}

interface

uses
  Classes, SysUtils;

type
  PNumByteArray = ^TNumByteArray;
  TNumByteArray = array[0..MaxInt] of Byte;

  { TFastBaseNumericList }

  generic TFastBaseNumericList<T> = class
  private
    FCount      : Integer;
    FCapacity   : Integer;
    FGrowthDelta: Integer;
    FSorted     : Boolean;

    FBaseList   : PNumByteArray;
    FItemSize   : Byte;
    FItemShift  : Byte;
    type
      PT = ^T;
      TTypeList = array[0..MaxInt shl 4] of T;
      PTypeList = ^TTypeList;
    function Get(Index : Integer) : T;
    function GetList : PTypeList; inline;
    // methods for sorted lists
    function IndexOfSorted(const aValue : T) : Integer;
    function IndexOfLeftMost(const aValue : T) : Integer;
    function IndexOfRightMost(const aValue : T) : Integer;

    procedure Put(Index : Integer; const AValue : T);
    procedure SetSorted(AValue : Boolean);
    procedure Expand(growValue : Integer);
  protected
    procedure SetCount(Val: Integer);
    procedure SetCapacity(NewCapacity: Integer); virtual;
    function  DoCompare(Item1, Item2 : Pointer) : Integer; virtual; abstract;
    procedure QSort(L : Integer; R : Integer); virtual;
    procedure DoSort; virtual;
  public
    constructor Create;
    destructor Destroy; override;

    property Items[Index: Integer]: T read Get write Put; default;
    property List : PTypeList read GetList;

    function DataSize: Integer; inline;
    procedure Flush;
    procedure Clear;

    procedure Add(const aValue : T);
    procedure AddSorted(const aValue : T);
    function  IndexOf(const aValue : T) : Integer; virtual;
    procedure Insert(const aValue : T; aIndex : Integer);
    procedure Delete(Index: Integer);
    procedure DeleteItems(Index: Integer; nbVals: Cardinal);
    procedure Exchange(index1, index2: Integer);
    procedure Sort; inline;

    property Count: Integer read FCount write SetCount;
    property Capacity: Integer read FCapacity write SetCapacity;

    property Sorted : Boolean read FSorted write SetSorted;
  end;

  { TFastByteList }

  TFastByteList = class(specialize TFastBaseNumericList<Byte>)
  protected
    function  DoCompare(Item1, Item2 : Pointer) : Integer; override;
  end;

  { TFastWordList }

  TFastWordList = class(specialize TFastBaseNumericList<Word>)
  protected
    function  DoCompare(Item1, Item2 : Pointer) : Integer; override;
  end;

  { TFastIntegerList }

  TFastIntegerList = class(specialize TFastBaseNumericList<Int32>)
  protected
    function  DoCompare(Item1, Item2 : Pointer) : Integer; override;
  end;

  { TFastCardinalList }

  TFastCardinalList = class(specialize TFastBaseNumericList<UInt32>)
  protected
    function  DoCompare(Item1, Item2 : Pointer) : Integer; override;
  end;

  { TFastInt64List }

  TFastInt64List = class(specialize TFastBaseNumericList<Int64>)
  protected
    function  DoCompare(Item1, Item2 : Pointer) : Integer; override;
  end;

  { TFastQWordList }

  TFastQWordList = class(specialize TFastBaseNumericList<UInt64>)
  protected
    function  DoCompare(Item1, Item2 : Pointer) : Integer; override;
  end;

  TKeyValuePair = packed record
    Key : QWord;
    case Byte of
    0: (Value: TObject);
    1: (PtrValue: Pointer);
    2: (Str : PChar);
    3: (WideStr : PWideChar);
  end;

  TKeyValuePairKind = (kvpkObject, kvpkPointer, kvpkString, kvpkWideString);

  { TFastKeyValuePairList }

  TFastKeyValuePairList = class(specialize TFastBaseNumericList<TKeyValuePair>)
  private
    FFreeValues : Boolean;
    FKeyValuePairKind : TKeyValuePairKind;
  protected
    function  DoCompare(Item1, Item2 : Pointer) : Integer; override;
  public
    constructor Create(aKind : TKeyValuePairKind;
                             aFreeValues : Boolean = true); overload;
    destructor Destroy; override;
    property ValueKind : TKeyValuePairKind read FKeyValuePairKind
                                           write FKeyValuePairKind;
    property FreeValues : Boolean read FFreeValues write FFreeValues;
  end;


function KeyValuePair(const aKey : QWord; aValue : TObject) : TKeyValuePair; overload;
function KeyValuePair(const aKey : QWord; aValue : Pointer) : TKeyValuePair; overload;
function KeyValuePair(const aKey : QWord; aValue : PChar) : TKeyValuePair; overload;
function KeyValuePair(const aKey : QWord; aValue : PWideChar) : TKeyValuePair; overload;
function KeyValuePair(const aKey : QWord; const aValue : String) : TKeyValuePair; overload;
function KeyValuePairWS(const aKey : QWord; const aValue : WideString) : TKeyValuePair;


implementation

uses Math;

function KeyValuePair(const aKey : QWord; aValue : TObject) : TKeyValuePair;
begin
  Result.Key := aKey;
  Result.Value := aValue;
end;

function KeyValuePair(const aKey : QWord; aValue : Pointer) : TKeyValuePair;
begin
  Result.Key := aKey;
  Result.PtrValue := aValue;
end;

function KeyValuePair(const aKey : QWord; aValue : PChar) : TKeyValuePair;
var sl : Cardinal;
begin
  Result.Key := aKey;
  sl := strlen(aValue)+1;
  Result.Str := StrAlloc(sl);
  if sl > 0 then Move(aValue^, Result.Str^, sl) else
                 Result.Str[0] := #0;
end;

function KeyValuePair(const aKey : QWord; aValue : PWideChar
  ) : TKeyValuePair;
var sl : Cardinal;
begin
  Result.Key := aKey;
  sl := strlen(aValue)+1;
  Result.WideStr := WideStrAlloc(sl);
  if sl > 0 then Move(aValue^, Result.WideStr^, sl shl 1) else
                 Result.WideStr[0] := #0;
end;

function KeyValuePair(const aKey : QWord; const aValue : String
  ) : TKeyValuePair;
var sl : Cardinal;
begin
  Result.Key := aKey;
  sl := Length(aValue);
  Result.Str := StrAlloc(sl+1);
  if sl > 0 then Move(aValue[1], Result.Str^, sl);
  Result.Str[sl] := #0;
end;

function KeyValuePairWS(const aKey : QWord; const aValue : WideString
  ) : TKeyValuePair;
var sl : Cardinal;
begin
  Result.Key := aKey;
  sl := Length(aValue);
  Result.WideStr := WideStrAlloc(sl + 1);
  if sl > 0 then Move(aValue[1], Result.WideStr^, sl shl 1);
  Result.WideStr[sl] := #0;
end;

{ TFastKeyValuePairList }

constructor TFastKeyValuePairList.Create(aKind : TKeyValuePairKind;
  aFreeValues : Boolean);
begin
  inherited Create;
  FFreeValues := aFreeValues;
  FKeyValuePairKind := aKind;
end;

function TFastKeyValuePairList.DoCompare(Item1, Item2 : Pointer) : Integer;
{$ifdef CPUX86_64}
assembler;
asm
  mov rax, [Item1]
  cmp rax, [Item2]
  je @eq
  ja @gr
  jmp @le
  @eq:
  xor eax, eax
  jmp @@end
  @gr:
  mov eax, 1
  jmp @@end
  @le:
  mov eax, -1
  @@end:
{$else}
begin
  Result := Math.CompareValue(PT(Item1)^.Key, PT(Item2)^.Key);
{$endif}
end;

destructor TFastKeyValuePairList.Destroy;
var i : integer;
begin
  if FreeValues then
  case FKeyValuePairKind of
    kvpkObject : begin
      for i := 0 to FCount-1 do
        List^[i].Value.Free;
    end;
    kvpkPointer : begin
      for i := 0 to FCount-1 do
        Freemem(List^[i].PtrValue);
    end;
    kvpkString : begin
      for i := 0 to FCount-1 do
        StrDispose(List^[i].Str);
    end;
    kvpkWideString : begin
      for i := 0 to FCount-1 do
        StrDispose(List^[i].WideStr);
    end;
  end;
  inherited Destroy;
end;

{ TFastInt64List }

function TFastInt64List.DoCompare(Item1, Item2 : Pointer) : Integer;
{$ifdef CPUX86_64}
assembler;
asm
  mov rax, [Item1]
  cmp rax, [Item2]
  je @eq
  jg @gr
  jmp @le
  @eq:
  xor eax, eax
  jmp @@end
  @gr:
  mov eax, 1
  jmp @@end
  @le:
  mov eax, -1
  @@end:
{$else}
begin
  Result := Math.CompareValue(PT(Item1)^, PT(Item2)^);
{$endif}
end;

{ TFastQWordList }

function TFastQWordList.DoCompare(Item1, Item2 : Pointer) : Integer;
{$ifdef CPUX86_64}
assembler;
asm
  mov rax, [Item1]
  cmp rax, [Item2]
  je @eq
  ja @gr
  jmp @le
  @eq:
  xor eax, eax
  jmp @@end
  @gr:
  mov eax, 1
  jmp @@end
  @le:
  mov eax, -1
  @@end:
{$else}
begin
  Result := Math.CompareValue(PT(Item1)^, PT(Item2)^);
{$endif}
end;

{ TFastCardinalList }

function TFastCardinalList.DoCompare(Item1, Item2 : Pointer) : Integer;
{$ifdef CPUX86_64}
assembler;
asm
  mov eax, dword [Item1]
  cmp eax, dword [Item2]
  je @eq
  ja @gr
  jmp @le
  @eq:
  xor eax, eax
  jmp @@end
  @gr:
  mov eax, 1
  jmp @@end
  @le:
  mov eax, -1
  @@end:
{$else}
begin
  Result := Math.CompareValue(PT(Item1)^, PT(Item2)^);
{$endif}
end;

{ TFastIntegerList }

function TFastIntegerList.DoCompare(Item1, Item2 : Pointer) : Integer;
{$ifdef CPUX86_64}
assembler;
asm
  mov eax, dword [Item1]
  cmp eax, dword [Item2]
  je @eq
  jg @gr
  jmp @le
  @eq:
  xor eax, eax
  jmp @@end
  @gr:
  mov eax, 1
  jmp @@end
  @le:
  mov eax, -1
  @@end:
{$else}
begin
  Result := Math.CompareValue(PT(Item1)^, PT(Item2)^);
{$endif}
end;

{ TFastWordList }

function TFastWordList.DoCompare(Item1, Item2 : Pointer) : Integer;
{$ifdef CPUX86_64}
assembler;
asm
  mov ax, word [Item1]
  cmp ax, word [Item2]
  je @eq
  ja @gr
  jmp @le
  @eq:
  xor eax, eax
  jmp @@end
  @gr:
  mov eax, 1
  jmp @@end
  @le:
  mov eax, -1
  @@end:
{$else}
begin
  Result := Math.CompareValue(PT(Item1)^, PT(Item2)^);
{$endif}
end;

{ TFastByteList }

function TFastByteList.DoCompare(Item1, Item2 : Pointer) : Integer;
{$ifdef CPUX86_64}
assembler;
asm
  mov al, byte [Item1]
  cmp al, byte [Item2]
  je @eq
  ja @gr
  jmp @le
  @eq:
  xor eax, eax
  jmp @@end
  @gr:
  mov eax, 1
  jmp @@end
  @le:
  mov eax, -1
  @@end:
{$else}
begin
  Result := Math.CompareValue(PT(Item1)^, PT(Item2)^);
{$endif}
end;

{ TFastBaseNumericList }

destructor TFastBaseNumericList.Destroy;
begin
  Clear;
  inherited;
end;

function TFastBaseNumericList.GetList : PTypeList;
begin
  Result := PTypeList(FBaseList);
end;

function TFastBaseNumericList.Get(Index : Integer) : T;
begin
  Result := PTypeList(FBaseList)^[Index];
end;

procedure TFastBaseNumericList.Put(Index : Integer; const AValue : T);
begin
  PTypeList(FBaseList)^[Index] := AValue;
end;

procedure TFastBaseNumericList.SetSorted(AValue : Boolean);
begin
  if FSorted = AValue then Exit;
  if AValue then
    Sort
  else
    FSorted := AValue;
end;

procedure TFastBaseNumericList.Expand(growValue : Integer);
begin
  while growValue > 0 do
  begin
    SetCapacity(FCapacity + FGrowthDelta);
    Dec(growValue, FGrowthDelta);
    if FGrowthDelta < $FFFF then FGrowthDelta := FGrowthDelta shl 1;
  end;
end;

procedure TFastBaseNumericList.QSort(L : Integer; R : Integer);
var
  I, J, Pivot: Integer;
  PivotObj : Pointer;
  O1 : T;
begin
  repeat
    I := L;
    J := R;
    Pivot := (L + R) shr 1;
    PivotObj := @(List^[Pivot]);
    repeat
      while (I <> Pivot) and (DoCompare(@(List^[i]), PivotObj) < 0) do Inc(I);
      while (J <> Pivot) and (DoCompare(@(List^[j]), PivotObj) > 0) do Dec(J);
      if I <= J then
      begin
        if I < J then
        begin
          O1 := List^[J];
          List^[J] := List^[i];
          List^[i] := O1;
        end;
        if Pivot = I then
        begin
          Pivot := J;
          PivotObj := @(List^[Pivot]);
        end
        else if Pivot = J then
        begin
          Pivot := I;
          PivotObj := @(List^[Pivot]);
        end;
        Inc(I);
        Dec(j);
      end;
    until I > J;
    if L < J then
      QSort(L, J);
    L := I;
  until I >= R;
end;

procedure TFastBaseNumericList.SetCount(Val: Integer);
begin
  Assert(Val >= 0);
  if Val > FCapacity then
    SetCapacity(Val);
  FCount := Val;
end;

procedure TFastBaseNumericList.SetCapacity(NewCapacity : Integer);
begin
  if newCapacity <> FCapacity then
  begin
    ReallocMem(FBaseList, newCapacity shl FItemShift);
    FCapacity := newCapacity;
  end;
end;

procedure TFastBaseNumericList.DoSort;
begin
  QSort(0, FCount-1);
end;

function TFastBaseNumericList.DataSize: Integer;
begin
  Result := FCount shl FItemShift;
end;

constructor TFastBaseNumericList.Create;
var c : Byte;
begin
  FItemSize := SizeOf(T);
  c := FItemSize shr 1;
  FItemShift := 0;
  while c > 0 do
  begin
    c := c shr 1;
    Inc(FItemShift);
  end;
  FGrowthDelta := 16;
end;

procedure TFastBaseNumericList.Flush;
begin
  SetCount(0);
end;

procedure TFastBaseNumericList.Clear;
begin
  SetCount(0);
  SetCapacity(0);
end;

procedure TFastBaseNumericList.Add(const aValue : T);
begin
  if FCapacity <= FCount then Expand(1);
  List^[FCount] := aValue;
  Inc(FCount);
end;

procedure TFastBaseNumericList.AddSorted(const aValue : T);
var i : Integer;
begin
  if FCapacity <= FCount then Expand(1);
  if (not FSorted) or (FCount = 0) then begin
    Add(aValue);
    Exit;
  end;
  i := IndexOfRightMost(aValue);
  Insert(aValue, i + 1);
end;

function TFastBaseNumericList.IndexOfLeftMost(const aValue : T) : Integer;
var R, m : Integer;
begin
  if (FCount = 0) or (DoCompare(@(List^[FCount-1]), @aValue) < 0) then Exit(FCount);
  Result := 0;
  R := FCount;
  while Result < R do
  begin
    m := (Result + R) shr 1;
    if DoCompare(@(List^[m]), @aValue) < 0  then
      Result := m + 1 else
      R := m;
  end;
end;

function TFastBaseNumericList.IndexOfRightMost(const aValue : T) : Integer;
var L, m : Integer;
begin
  if (FCount = 0) or (DoCompare(@(List^[0]), @aValue) > 0) then Exit(-1);
  L := 0;
  Result := FCount;
  while L < Result do
  begin
    m := (Result + L) shr 1;
    if DoCompare(@(List^[m]), @aValue) > 0 then
      Result := m else
      L := m + 1;
  end;
  Dec(Result);
end;

function TFastBaseNumericList.IndexOfSorted(const aValue : T) : Integer;
var R, L : Integer;
begin
  L := 0;
  R := FCount - 1;
  while L < R do
  begin
    Result := (L + R) shr 1;
    if DoCompare(@(List^[Result]), @aValue) < 0 then
      L := Result + 1 else
    if DoCompare(@(List^[Result]), @aValue) > 0 then
      R := Result - 1 else
      Exit;
  end;
  Result := -1;
end;

function TFastBaseNumericList.IndexOf(const aValue : T) : Integer;
var i : integer;
begin
  if FSorted and (FCount > 4) then
    Result := IndexOfSorted(aValue) else
  begin
    Result := -1;
    for i := 0 to FCount-1 do
      if DoCompare(@(List^[i]), @aValue) = 0 then Exit(i);
  end;
end;

procedure TFastBaseNumericList.Insert(const aValue : T; aIndex : Integer);
begin
  if FCount >= FCapacity then Expand(1);

  if aIndex < FCount then
    System.Move(List^[aIndex], List^[aIndex + 1],
      (FCount - aIndex) shl FItemShift);
  List^[aIndex] := aValue;
  Inc(FCount);
end;

procedure TFastBaseNumericList.Delete(Index: Integer);
begin
  Dec(FCount);
  if Index < FCount then
    System.Move(List^[Index + 1], List^[Index], (FCount - Index) shl FItemShift);
end;

procedure TFastBaseNumericList.DeleteItems(Index: Integer; nbVals: Cardinal);
begin
  if nbVals > 0 then
  begin
    if Index + Integer(nbVals) < FCount then
    begin
      System.Move(List^[Index + Integer(nbVals)], List^[Index],
                             (FCount - Index - Integer(nbVals)) shl FItemShift);
    end;
    Dec(FCount, nbVals);
  end;
end;

procedure TFastBaseNumericList.Exchange(index1, index2: Integer);
var m : T;
begin
  m := List^[index1];
  List^[index1] := List^[index2];
  List^[index2] := m;
end;

procedure TFastBaseNumericList.Sort;
begin
  if FCount > 1 then DoSort;
  FSorted := true;
end;

end.

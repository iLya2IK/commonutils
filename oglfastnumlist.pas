{ OGLFastNumList
   lightweighted numeric lists
   Copyright (c) 2021 Ilya Medvedkov   }

unit OGLFastNumList;

{$mode objfpc}
{$ifdef CPUX86_64}
{$asmMode intel}
{$endif}
{$inline on}

interface

uses
  Classes, SysUtils, Variants;

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
      TTypeList = array[0..MaxInt shr 4] of T;
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
    procedure DeleteAll; virtual;
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
    procedure AddEmptyValues(nbVals : Cardinal);
    function  IndexOf(const aValue : T) : Integer; virtual;
    procedure Insert(const aValue : T; aIndex : Integer);
    procedure Delete(Index: Integer); virtual;
    procedure DeleteItems(Index: Integer; nbVals: Cardinal); virtual;
    procedure Exchange(index1, index2: Integer);
    procedure Sort;

    property Count: Integer read FCount;
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
    Key : PtrUInt;
    case Byte of
    0: (Value: TObject);
    1: (PtrValue: Pointer);
    2: (Str : PChar);
    3: (WideStr : PWideChar);
    4: (PVar : PVariant);
    5: (UIntValue : PtrUInt);
    6: (IntValue : PtrInt);
  end;
  PKeyValuePair = ^TKeyValuePair;

  { TFastKeyValuePairList }

  generic TFastKeyValuePairList<TK> = class(specialize TFastBaseNumericList<TKeyValuePair>)
  private
    FFreeValues : Boolean;
    type PTK = ^TK;
    function GetValue(aKey : QWord): TK;
    procedure SetValue(aKey : QWord; const AValue: TK);
  protected
    function  DoGetValue(aIndex : Integer) : TK; virtual;
    procedure DoSetValue(aIndex : Integer; const AValue : TK); virtual;
    function  DoCompare(Item1, Item2 : Pointer) : Integer; override;
    procedure DisposeValue(Index : Integer); virtual; abstract;
    class function NullValue : TK; virtual; abstract;
    procedure DeleteAll; override;
  public
    constructor Create(aFreeValues : Boolean = true); overload;
    destructor Destroy; override;

    procedure Delete(Index: Integer); override;
    procedure DeleteItems(Index: Integer; nbVals: Cardinal); override;

    procedure AddKey(const aKey : QWord; const aValue : TK); virtual; abstract;
    procedure AddKeySorted(const aKey : QWord; const aValue : TK); virtual; abstract;

    procedure AddInt(const aKey : QWord; aValue : PtrInt);
    procedure AddIntSorted(const aKey : QWord; aValue : PtrInt);
    procedure AddUInt(const aKey : QWord; aValue : PtrUInt);
    procedure AddUintSorted(const aKey : QWord; aValue : PtrUInt);
    procedure AddObj(const aKey : QWord; aValue : TObject);
    procedure AddObjSorted(const aKey : QWord; aValue : TObject);
    procedure AddPtr(const aKey : QWord; aValue : Pointer);
    procedure AddPtrSorted(const aKey : QWord; aValue : Pointer);
    { NB: all the PChars and PWideChars are completely copied in the AddStr
      method not just assigned to the TKeyValuePair.Str/WideStr field.
      This means that it is strongly recommended that you set the FreeValues
      field to true if you assums to use the AddStr(QWord, PChar) and
      AddWStr(QWord, PWideChar) methods.
      If you do not want to create a new instance for the passed PChar or
      PWideChar parameter, but to hold only the copy of that pointers in the
      list, just use the AddPtr/AddPtrSorted methods.
      To avoid that misunderstanding, this module does not have TFastMapPChar
      and TFastMapPWideChar, and I recommend using TFastMapPtr to store pointers
      to PChars and PWideChars.}
    procedure AddStr(const aKey : QWord; aValue : PChar);
    procedure AddStrSorted(const aKey : QWord; aValue : PChar);
    procedure AddStr(const aKey : QWord; const aValue : String); overload;
    procedure AddStrSorted(const aKey : QWord; const aValue : String); overload;
    procedure AddWStr(const aKey : QWord; aValue : PWideChar);
    procedure AddWStrSorted(const aKey : QWord; aValue : PWideChar);
    procedure AddWStr(const aKey : QWord; const aValue : WideString); overload;
    procedure AddWStrSorted(const aKey : QWord; const aValue : WideString); overload;
    procedure AddVariant(const aKey : QWord; const aValue : Variant);
    procedure AddVariantSorted(const aKey : QWord; const aValue : Variant);

    property Value[aKey : QWord] : TK read GetValue write SetValue;
    property FreeValues : Boolean read FFreeValues write FFreeValues;
  end;

  { TFastMapObj }

  TFastMapObj = class(specialize TFastKeyValuePairList<TObject>)
  protected
    procedure DisposeValue(Index : Integer); override;
    class function NullValue : TObject; override;
  public
    procedure AddKey(const aKey : QWord; const aValue : TObject); override;
    procedure AddKeySorted(const aKey : QWord; const aValue : TObject); override;
  end;

  { TFastMapPtr }

  TFastMapPtr = class(specialize TFastKeyValuePairList<Pointer>)
  protected
    procedure DisposeValue(Index : Integer); override;
    class function NullValue : Pointer; override;
  public
    procedure AddKey(const aKey : QWord; const aValue : Pointer); override;
    procedure AddKeySorted(const aKey : QWord; const aValue : Pointer); override;
  end;

  { TFastMapUInt }

  TFastMapUInt = class(specialize TFastKeyValuePairList<PtrUInt>)
  protected
    procedure DisposeValue({%H-}Index : Integer); override;
    class function NullValue : PtrUInt; override;
  public
    constructor Create; overload;
    procedure AddKey(const aKey : QWord; const aValue : PtrUInt); override;
    procedure AddKeySorted(const aKey : QWord; const aValue : PtrUInt); override;
  end;

  { TFastMapInt }

  TFastMapInt = class(specialize TFastKeyValuePairList<PtrInt>)
  protected
    procedure DisposeValue({%H-}Index : Integer); override;
    class function NullValue : PtrInt; override;
  public
    constructor Create; overload;
    procedure AddKey(const aKey : QWord; const aValue : PtrInt); override;
    procedure AddKeySorted(const aKey : QWord; const aValue : PtrInt); override;
  end;

  { TFastMapVar }

  TFastMapVar = class(specialize TFastKeyValuePairList<Variant>)
  protected
    procedure DisposeValue(Index : Integer); override;
    function DoGetValue(aIndex : Integer) : Variant; override;
    procedure DoSetValue(aIndex : Integer; const AValue : Variant); override;
    class function NullValue : Variant; override;
  public
    procedure AddKey(const aKey : QWord; const aValue : Variant); override;
    procedure AddKeySorted(const aKey : QWord; const aValue : Variant); override;
  end;

  { TFastMapStr }

  TFastMapStr = class(specialize TFastKeyValuePairList<String>)
  protected
    procedure DisposeValue(Index : Integer); override;
    function DoGetValue(aIndex : Integer) : String; override;
    procedure DoSetValue(aIndex : Integer; const AValue : String); override;
    class function NullValue : String; override;
  public
    procedure AddKey(const aKey : QWord; const aValue : String); override;
    procedure AddKeySorted(const aKey : QWord; const aValue : String); override;
  end;

  { TFastMapWStr }

  TFastMapWStr = class(specialize TFastKeyValuePairList<WideString>)
  protected
    procedure DisposeValue(Index : Integer); override;
    function DoGetValue(aIndex : Integer) : WideString; override;
    procedure DoSetValue(aIndex : Integer; const AValue : WideString); override;
    class function NullValue : WideString; override;
  public
    procedure AddKey(const aKey : QWord; const aValue : WideString); override;
    procedure AddKeySorted(const aKey : QWord; const aValue : WideString); override;
  end;

function KeyValuePair(const aKey : QWord; aValue : PtrUInt) : TKeyValuePair; overload;
function KeyValuePair(const aKey : QWord; aValue : PtrInt) : TKeyValuePair; overload;
function KeyValuePair(const aKey : QWord; aValue : TObject) : TKeyValuePair; overload;
function KeyValuePair(const aKey : QWord; aValue : Pointer) : TKeyValuePair; overload;
function KeyValuePair(const aKey : QWord; aValue : PChar) : TKeyValuePair; overload;
function KeyValuePair(const aKey : QWord; aValue : PWideChar) : TKeyValuePair; overload;
function KeyValuePair(const aKey : QWord; const aValue : String) : TKeyValuePair; overload;
function KeyValuePairWS(const aKey : QWord; const aValue : WideString) : TKeyValuePair;
function KeyValuePairVar(const aKey : QWord; const aValue : Variant) : TKeyValuePair;
function DoKeyValueCompare(Item1, Item2 : Pointer) : Integer;

implementation

uses Math;

function AllocVariant(const aValue : Variant) : PVariant;
begin
  Result := AllocMem(SizeOf(Variant));
  Result^ := aValue;
end;

function AllocString(const aValue : String) : PChar;
var sl : Cardinal;
begin
  sl := Length(aValue);
  Result := StrAlloc(sl + 1);
  if sl > 0 then Move(aValue[1], Result^, sl);
  Result[sl] := #0;
end;

function AllocWideString(const aValue : WideString) : PWideChar;
var sl : Cardinal;
begin
  sl := Length(aValue);
  Result := WideStrAlloc(sl + 1);
  if sl > 0 then Move(aValue[1], Result^, sl shl 1);
  Result[sl] := #0;
end;

function AllocChar(aValue : PChar) : PChar;
var sl : Cardinal;
begin
  sl := strlen(aValue);
  Result := StrAlloc(sl + 1);
  if sl > 0 then Move(aValue^, Result^, sl);
  Result[sl] := #0;
end;

function AllocWideChar(aValue : PWideChar) : PWideChar;
var sl : Cardinal;
begin
  sl := strlen(aValue);
  Result := WideStrAlloc(sl + 1);
  if sl > 0 then Move(aValue^, Result^, sl shl 1);
  Result[sl] := #0;
end;

function KeyValuePair(const aKey: QWord; aValue: PtrUInt): TKeyValuePair;
begin
  Result.Key := aKey;
  Result.UIntValue := aValue;
end;

function KeyValuePair(const aKey: QWord; aValue: PtrInt): TKeyValuePair;
begin
  Result.Key := aKey;
  Result.IntValue := aValue;
end;

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
begin
  Result.Key := aKey;
  Result.Str := AllocChar(aValue);
end;

function KeyValuePair(const aKey : QWord; aValue : PWideChar
  ) : TKeyValuePair;
begin
  Result.Key := aKey;
  Result.WideStr := AllocWideChar(aValue);
end;

function KeyValuePair(const aKey : QWord; const aValue : String
  ) : TKeyValuePair;
begin
  Result.Key := aKey;
  Result.Str := AllocString(aValue);
end;

function KeyValuePairWS(const aKey : QWord; const aValue : WideString
  ) : TKeyValuePair;
begin
  Result.Key := aKey;
  Result.WideStr := AllocWideString(aValue);
end;

function KeyValuePairVar(const aKey : QWord; const aValue : Variant
  ) : TKeyValuePair;
begin
  Result.Key := aKey;
  Result.PVar := AllocVariant(aValue);
end;

function DoKeyValueCompare(Item1, Item2: Pointer): Integer;
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
  Result := Math.CompareValue(PKeyValuePair(Item1)^.Key, PKeyValuePair(Item2)^.Key);
{$endif}
end;

{ TFastMapWStr }

procedure TFastMapWStr.DisposeValue(Index: Integer);
begin
  StrDispose(PKeyValuePair(FBaseList)[Index].WideStr);
end;

function TFastMapWStr.DoGetValue(aIndex: Integer): WideString;
begin
  Result := StrPas(PKeyValuePair(FBaseList)[aIndex].WideStr);
end;

procedure TFastMapWStr.DoSetValue(aIndex: Integer; const AValue: WideString);
begin
  PKeyValuePair(FBaseList)[aIndex].WideStr := AllocWideString(aValue);
end;

class function TFastMapWStr.NullValue: WideString;
begin
  Result := '';
end;

procedure TFastMapWStr.AddKey(const aKey: QWord; const aValue: WideString);
begin
  AddWStr(aKey, aValue);
end;

procedure TFastMapWStr.AddKeySorted(const aKey: QWord; const aValue: WideString
  );
begin
  AddWStrSorted(aKey, aValue);
end;

{ TFastMapStr }

procedure TFastMapStr.DisposeValue(Index: Integer);
begin
  StrDispose(PKeyValuePair(FBaseList)[Index].Str);
end;

function TFastMapStr.DoGetValue(aIndex: Integer): String;
begin
  Result := StrPas(PKeyValuePair(FBaseList)[aIndex].Str);
end;

procedure TFastMapStr.DoSetValue(aIndex: Integer; const AValue: String);
begin
  PKeyValuePair(FBaseList)[aIndex].Str := AllocString(aValue);
end;

class function TFastMapStr.NullValue: String;
begin
  Result := '';
end;

procedure TFastMapStr.AddKey(const aKey: QWord; const aValue: String);
begin
  AddStr(aKey, aValue);
end;

procedure TFastMapStr.AddKeySorted(const aKey: QWord; const aValue: String);
begin
  AddStrSorted(aKey, aValue);
end;

{ TFastMapUInt }

procedure TFastMapUInt.DisposeValue({%H-}Index: Integer);
begin
  // do nothing
end;

class function TFastMapUInt.NullValue: PtrUInt;
begin
  Result := 0;
end;

constructor TFastMapUInt.Create;
begin
  inherited Create(false);
end;

procedure TFastMapUInt.AddKey(const aKey: QWord; const aValue: PtrUInt);
begin
  AddUInt(aKey, AValue);
end;

procedure TFastMapUInt.AddKeySorted(const aKey: QWord; const aValue: PtrUInt);
begin
  AddUIntSorted(aKey, AValue);
end;

{ TFastMapInt }

procedure TFastMapInt.DisposeValue({%H-}Index: Integer);
begin
  // do nothing
end;

class function TFastMapInt.NullValue: PtrInt;
begin
  Result := 0;
end;

constructor TFastMapInt.Create;
begin
  inherited Create(false);
end;

procedure TFastMapInt.AddKey(const aKey: QWord; const aValue: PtrInt);
begin
  AddInt(aKey, aValue);
end;

procedure TFastMapInt.AddKeySorted(const aKey: QWord; const aValue: PtrInt);
begin
  AddIntSorted(aKey, aValue);
end;

{ TFastMapPtr }

procedure TFastMapPtr.DisposeValue(Index: Integer);
begin
  FreeMem(PKeyValuePair(FBaseList)[Index].PtrValue);
end;

class function TFastMapPtr.NullValue: Pointer;
begin
  Result := nil;
end;

procedure TFastMapPtr.AddKey(const aKey: QWord; const aValue: Pointer);
begin
  AddPtr(aKey, aValue);
end;

procedure TFastMapPtr.AddKeySorted(const aKey: QWord; const aValue: Pointer);
begin
  AddPtrSorted(aKey, aValue);
end;

{ TFastMapVar }

procedure TFastMapVar.DisposeValue(Index: Integer);
begin
  VarClear(PKeyValuePair(FBaseList)[Index].PVar^);
  FreeMem(PKeyValuePair(FBaseList)[Index].PVar);
end;

function TFastMapVar.DoGetValue(aIndex: Integer): Variant;
begin
  Result := PKeyValuePair(FBaseList)[aIndex].PVar^;
end;

procedure TFastMapVar.DoSetValue(aIndex: Integer; const AValue: Variant);
begin
  PKeyValuePair(FBaseList)[aIndex].PVar := AllocVariant(AValue);
end;

class function TFastMapVar.NullValue: Variant;
begin
  Result := Null;
end;

procedure TFastMapVar.AddKey(const aKey: QWord; const aValue: Variant);
begin
  AddVariant(aKey, aValue);
end;

procedure TFastMapVar.AddKeySorted(const aKey: QWord; const aValue: Variant);
begin
  AddVariantSorted(aKey, aValue);
end;

{ TFastMapObj }

procedure TFastMapObj.DisposeValue(Index: Integer);
begin
  PKeyValuePair(FBaseList)[Index].Value.Free
end;

class function TFastMapObj.NullValue: TObject;
begin
  Result := nil;
end;

procedure TFastMapObj.AddKey(const aKey: QWord; const aValue: TObject);
begin
  AddObj(aKey, aValue);
end;

procedure TFastMapObj.AddKeySorted(const aKey: QWord; const aValue: TObject);
begin
  AddObjSorted(aKey, aValue);
end;

{ TFastKeyValuePairList }

constructor TFastKeyValuePairList.Create(aFreeValues: Boolean);
begin
  inherited Create;
  FFreeValues := aFreeValues;
end;

function TFastKeyValuePairList.DoGetValue(aIndex : Integer) : TK;
begin
  Result := PTK(FBaseList + aIndex shl FItemShift + SizeOf(PtrUInt))^;
end;

procedure TFastKeyValuePairList.DoSetValue(aIndex : Integer; const AValue : TK);
begin
  PTK(FBaseList + aIndex shl FItemShift + SizeOf(PtrUInt))^ := AValue;
end;

function TFastKeyValuePairList.GetValue(aKey: QWord): TK;
var Index : Integer;
begin
  Result := NullValue;
  Index := IndexOf(KeyValuePair(aKey, nil));
  if Index >= 0 then
    Result := DoGetValue(Index);
end;

procedure TFastKeyValuePairList.SetValue(aKey: QWord; const AValue: TK);
var Index : Integer;
begin
  Index := IndexOf(KeyValuePair(aKey, nil));
  if Index < 0 then
    AddKeySorted(aKey, AValue) else
  begin
    if FFreeValues then DisposeValue(Index);
    DoSetValue(Index, AValue);
  end;
end;

procedure TFastKeyValuePairList.DeleteAll;
var i : integer;
begin
  if FreeValues then
   for i := 0 to FCount-1 do
     DisposeValue(I);
  inherited DeleteAll;
end;

function TFastKeyValuePairList.DoCompare(Item1, Item2: Pointer): Integer;
begin
  Result := DoKeyValueCompare(Item1, Item2);
end;

destructor TFastKeyValuePairList.Destroy;
begin
  Flush;
  inherited Destroy;
end;

procedure TFastKeyValuePairList.Delete(Index : Integer);
begin
  if FreeValues then
    DisposeValue(Index);
  inherited Delete(Index);
end;

procedure TFastKeyValuePairList.DeleteItems(Index : Integer; nbVals : Cardinal);
var i : integer;
begin
  if FreeValues then
    for i := Index to Index + nbVals - 1 do
      DisposeValue(i);
  inherited DeleteItems(Index, nbVals);
end;

procedure TFastKeyValuePairList.AddInt(const aKey: QWord; aValue: PtrInt);
begin
  Add(KeyValuePair(aKey, aValue));
end;

procedure TFastKeyValuePairList.AddIntSorted(const aKey: QWord; aValue: PtrInt);
begin
  AddSorted(KeyValuePair(aKey, aValue));
end;

procedure TFastKeyValuePairList.AddUInt(const aKey: QWord; aValue: PtrUInt);
begin
  Add(KeyValuePair(aKey, aValue));
end;

procedure TFastKeyValuePairList.AddUintSorted(const aKey: QWord; aValue: PtrUInt);
begin
  AddSorted(KeyValuePair(aKey, aValue));
end;

procedure TFastKeyValuePairList.AddObj(const aKey : QWord; aValue : TObject);
begin
  Add(KeyValuePair(aKey, aValue));
end;

procedure TFastKeyValuePairList.AddObjSorted(const aKey : QWord;
  aValue : TObject);
begin
  AddSorted(KeyValuePair(aKey, aValue));
end;

procedure TFastKeyValuePairList.AddPtr(const aKey : QWord; aValue : Pointer);
begin
  Add(KeyValuePair(aKey, aValue));
end;

procedure TFastKeyValuePairList.AddPtrSorted(const aKey : QWord;
  aValue : Pointer);
begin
  AddSorted(KeyValuePair(aKey, aValue));
end;

procedure TFastKeyValuePairList.AddStr(const aKey : QWord; aValue : PChar);
begin
  Add(KeyValuePair(aKey, aValue));
end;

procedure TFastKeyValuePairList.AddStrSorted(const aKey : QWord; aValue : PChar
  );
begin
  AddSorted(KeyValuePair(aKey, aValue));
end;

procedure TFastKeyValuePairList.AddStr(const aKey : QWord; const aValue : String);
begin
  Add(KeyValuePair(aKey, aValue));
end;

procedure TFastKeyValuePairList.AddStrSorted(const aKey : QWord;
  const aValue : String);
begin
  AddSorted(KeyValuePair(aKey, aValue));
end;

procedure TFastKeyValuePairList.AddWStr(const aKey : QWord; aValue : PWideChar);
begin
  Add(KeyValuePair(aKey, aValue));
end;

procedure TFastKeyValuePairList.AddWStrSorted(const aKey : QWord;
  aValue : PWideChar);
begin
  AddSorted(KeyValuePair(aKey, aValue));
end;

procedure TFastKeyValuePairList.AddWStr(const aKey : QWord;
  const aValue : WideString);
begin
  Add(KeyValuePairWS(aKey, aValue));
end;

procedure TFastKeyValuePairList.AddWStrSorted(const aKey : QWord;
  const aValue : WideString);
begin
  AddSorted(KeyValuePairWS(aKey, aValue));
end;

procedure TFastKeyValuePairList.AddVariant(const aKey : QWord;
  const aValue : Variant);
begin
  Add(KeyValuePairVar(aKey, aValue));
end;

procedure TFastKeyValuePairList.AddVariantSorted(const aKey : QWord;
  const aValue : Variant);
begin
  AddSorted(KeyValuePairVar(aKey, aValue));
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

procedure TFastBaseNumericList.DeleteAll;
begin
  FCount := 0;
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
  DeleteAll;
end;

procedure TFastBaseNumericList.Clear;
begin
  DeleteAll;
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
  if (not FSorted) or (FCount = 0) then begin
    Add(aValue);
    Exit;
  end;
  if FCapacity <= FCount then Expand(1);
  i := IndexOfRightMost(aValue);
  Insert(aValue, i + 1);
end;

procedure TFastBaseNumericList.AddEmptyValues(nbVals : Cardinal);
begin
  if FCapacity < (FCount + nbVals) then Expand(nbVals + FCount - FCapacity);
  Inc(FCount, nbVals);
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
var R, L, C : Integer;
begin
  L := 0;
  R := FCount - 1;
  while L < R do
  begin
    Result := (L + R) shr 1;
    C := DoCompare(@(List^[Result]), @aValue);
    if C < 0 then
      L := Result + 1 else
    if C > 0 then
      R := Result - 1 else
      Exit;
  end;
  if DoCompare(@(List^[L]), @aValue) = 0 then
    Result := L else
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

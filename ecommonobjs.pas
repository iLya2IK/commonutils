{
 ECommonObjs:
   Thread safe helpers to access to objects and data

   Part of ESolver project

   Copyright (c) 2019-2021 by Ilya Medvedkov

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
}

unit ECommonObjs;

{$mode objfpc}{$H+}

interface

uses
  Classes, SysUtils, OGLFastList, extmemorystream;

type
  { TNetCustomLockedObject }

  TNetCustomLockedObject = class
  private
    FRTI: TRTLCriticalSection;
  public
    constructor Create;
    destructor Destroy; override;
    procedure Lock;
    procedure UnLock;
  end;

  { TNetReferencedObject }

  TNetReferencedObject = class(TNetCustomLockedObject)
  private
    mReferenceCount : Integer;
  public
    constructor Create;
    procedure IncReference;
    procedure DecReference;
    function HasReferences : Boolean;
    function HasExternReferences : Boolean;
  end;

  TNetReferenceOnDisconnect = procedure (v : TNetReferencedObject) of object;

  { TNetReferenceHolderList }

  TNetReferenceHolderList = class(TNetCustomLockedObject)
  private
    FList : TFastList;
    FOnDisconnect :  TNetReferenceOnDisconnect;
    function GetCount: integer;
    function GetItem(index : integer): TNetReferencedObject;
  public
    constructor Create;
    destructor Destroy; override;
    procedure Clear;
    procedure Add(Obj : TNetReferencedObject);
    procedure Remove(Obj : TNetReferencedObject);
    procedure Delete(i : integer);
    function IndexOf(Obj : TNetReferencedObject) : integer;
    property Count : integer read GetCount;
    property OnDisconnect : TNetReferenceOnDisconnect read FOnDisconnect write FOnDisconnect;
    property Item[index : integer] : TNetReferencedObject read GetItem; default;
  end;

  THandlesList = class;

  { TNetHandle }

  TNetHandle = class(TNetReferencedObject)
  private
   fHandle : Cardinal;
   fOwner  : THandlesList;
  public
   procedure Disconnect;
   property Handle : Cardinal read FHandle write FHandle;
   property Owner : THandlesList read fOwner write fOwner;
  end;

  { THandlesList }

  THandlesList = class(TNetCustomLockedObject)
  private
    FList : TNetReferenceHolderList;
    function GetCount: integer;
    function GetItem(index : integer): TNetHandle;
  public
    constructor Create;
    destructor Destroy; override;

    function  GetByHandle(aHandle : Cardinal) : TObject;
    procedure RemoveHandle(obj : TNetHandle); virtual;
    procedure AddHandle(Obj : TNetHandle); virtual;
    procedure Clear; virtual;

    property Count : integer read GetCount;
    property Item[index : integer] : TNetHandle read GetItem; default;
  end;

  { TThreadSafeObject }

  TThreadSafeObject = class(TNetCustomLockedObject);

  { TThreadGetValue }

  generic TThreadGetValue<T> = class(TThreadSafeObject)
  private
    FValue : T;
    function GetValue: T;
  public
    constructor Create(const AValue : T);
    property Value : T read GetValue;
  end;

  { TThreadGetSetValue }

  generic TThreadGetSetValue<T> = class(specialize TThreadGetValue<T>)
  private
    procedure SetValue(const AValue: T);
  public
    property Value : T read GetValue write SetValue;
  end;

  { TThreadNumeric }

  generic TThreadNumeric<T> = class(specialize TThreadGetSetValue<T>)
  public
    procedure IncValue; overload;
    procedure DecValue; overload;
    procedure IncValue(IncSz : T); overload;
    procedure DecValue(DecSz: T); overload;
  end;

  TThreadByte = class(specialize TThreadNumeric<Byte>);
  TThreadWord = class(specialize TThreadNumeric<Word>);
  TThreadInteger = class(specialize TThreadNumeric<Integer>);
  TThreadCardinal = class(specialize TThreadNumeric<Cardinal>);
  TThreadQWord = class(specialize TThreadNumeric<QWord>);
  TThreadInt64 = class(specialize TThreadNumeric<Int64>);

  TThreadUtf8String = class(specialize TThreadGetSetValue<UTF8String>);
  TThreadWideString = class(specialize TThreadGetSetValue<WideString>);

  { TThreadPointer }

  TThreadPointer = class(specialize TThreadGetValue<Pointer>)
  public
    constructor Create(ASize : QWord);
    destructor Destroy; override;
    procedure Realloc(AValue : Pointer);
  end;

  { TThreadBoolean }

  TThreadBoolean = class(specialize TThreadGetSetValue<Boolean>)
  public
    procedure Enable;
    procedure Disable;
  end;

  { TThreadSafeAutoIncrementCardinal }

  TThreadSafeAutoIncrementCardinal = class(TThreadSafeObject)
  private
    FValue : Cardinal;
    function GetID: Cardinal;
  public
    constructor Create;
    procedure Reset;
    procedure SetValue(v : Cardinal);
    property ID : Cardinal read GetID;
  end;

  { TThreadStringList }

  TThreadStringList = class(TThreadSafeObject)
  private
    FStringList: TStringList;
    FOnChange : TNotifyEvent;
    FUpdates  : Integer;

    function GetCount: Integer;
    function GetDelimitedText : String;
    function GetDelimiter : Char;
    function GetStr(index : integer): String;
    function GetText: String;
    procedure SetDelimitedText(const AValue : String);
    procedure SetDelimiter(AValue : Char);
    procedure SetStr(index : integer; const AValue : String);
    procedure SetText(const AValue: String);
    procedure DoChange;
    function GetOnChange: TNotifyEvent;
    procedure SetOnChange(AValue: TNotifyEvent);
  public
    constructor Create;
    destructor Destroy; override;
    function Add(const S: string): Integer;
    procedure Delete(Index: Integer);
    procedure DeleteFromTo(Index1, Index2: Integer);
    function IndexOf(const S: string): Integer;
    procedure AddStrings(S : TStringList);
    procedure BeginUpdate;
    procedure EndUpdate;
    procedure Clear;
    property Item[index : integer] : String read GetStr write SetStr; default;
    property Count : Integer read GetCount;
    property Text : String read GetText write SetText;
    property DelimitedText : String read GetDelimitedText write SetDelimitedText;
    property Delimiter : Char read GetDelimiter write SetDelimiter;
    property OnChange : TNotifyEvent read GetOnChange write SetOnChange;
  end;

  { TThreadSafeFastBaseCollection }

  generic TThreadSafeFastBaseCollection<T> = class(TThreadSafeObject)
  private
    FCollection :  TFastCollection;

    function GetCount: integer;
    function GetObject(index : integer): T;
    procedure SetObject(index : integer; AValue: T);
  protected
    procedure SetCount(AValue: integer); virtual;
  public
    constructor Create;
    function Add(const Obj : T) : Integer; virtual;
    function IndexOf(const Obj : T) : Integer;
    function Remove(const obj: T) : integer; virtual;
    procedure Delete(Ind : integer); virtual;
    procedure DeleteFreeAssigned(Ind : integer); virtual;
    procedure Clear; virtual;
    procedure Extract(Ind : integer); virtual;
    procedure Pack; virtual;
    destructor Destroy; override;
    procedure SortList(func : TObjectSortFunction);

    property Count : integer read GetCount write SetCount;
    property Item[index : integer] : T read GetObject write SetObject; default;
  end;

  TThreadSafeFastCollection = class(specialize TThreadSafeFastBaseCollection<TObject>);

  { TThreadSafeFastBaseList }

  generic TThreadSafeFastBaseList<T> = class(TThreadSafeObject)
  private
    FList :  TFastList;

    function GetCount: integer;
    function GetObject(index : integer): T;
    procedure SetObject(index : integer; AValue: T);
  protected
    procedure SetCount(AValue: integer); virtual;
  public
    constructor Create;
    function Add(const Obj : T) : Integer; virtual;
    function IndexOf(const Obj : T) : Integer;
    function Remove(const obj: T) : integer; virtual;
    procedure Delete(Ind : integer); virtual;
    procedure Clear; virtual;
    procedure Extract(Ind : integer); virtual;
    procedure Pack; virtual;
    destructor Destroy; override;
    procedure SortList(func : TObjectSortFunction);

    property Count : integer read GetCount write SetCount;
    property Item[index : integer] : T read GetObject write SetObject; default;
  end;

  TThreadSafeFastList = class(specialize TThreadSafeFastBaseList<TObject>);

  { TThreadSafeFastBaseSeq }

  generic TThreadSafeFastBaseSeq<T> = class(TThreadSafeObject)
  private
    FSeq : TFastSeq;
    function GetCount: integer;
  public
    constructor Create;
    destructor Destroy; override;

    procedure Clean;
    procedure ExtractAll;
    procedure Push_back(const O : TObject); virtual;
    procedure Push_front(const O : TObject); virtual;
    function Pop : TIteratorObject;
    function PopValue : T;
    function LastValue : T;
    function FirstValue : T;
    function InsertBefore(loc: TIteratorObject; o: TObject): TIteratorObject; virtual;
    function InsertAfter(loc: TIteratorObject; o: TObject): TIteratorObject; virtual;
    procedure Erase(const loc : TIteratorObject);
    procedure EraseObject(const obj : TObject);
    function  EraseObjectsByCriteria(criteria: TFindObjectCriteria;
                                               data : pointer): Boolean;
    function FindValue(criteria : TFindObjectCriteria; data : Pointer) : T;
    procedure DoForAll(action: TObjectAction);
    procedure DoForAllEx(action: TExObjectAction; data : Pointer);
    procedure Extract(const loc: TIteratorObject);
    procedure ExtractObject(const obj: TObject);
    function  ExtractObjectsByCriteria(criteria: TFindObjectCriteria;
                                       afterextract : TObjectAction;
                                               data : pointer): Boolean;
    function ListBegin : TIteratorObject;
    function IteratorBegin : TIterator;
    class function ListEnd   : TIteratorObject;

    property Count : integer read GetCount;
  end;

  TThreadSafeFastSeq = class(specialize TThreadSafeFastBaseSeq<TObject>);

  { TNetReferenceList }

  TNetReferenceList = class(TThreadSafeFastSeq)
  private
    function IfNoReferences(obj : Tobject; ptr : pointer) : Boolean;
  public
    destructor Destroy; override;
    procedure Add(const O : TNetReferencedObject);
    procedure CleanDead;
  end;

  { TReferencedStream }

  TReferencedStream = class(TNetReferencedObject)
  private
    FStream : TStream;
  public
    constructor Create(aStrm : TStream);
    destructor Destroy; override;
    procedure WriteTo(Strm : TStream; from, Sz : PtrInt); virtual;
    property Stream : TStream read FStream;
  end;

  { TRefMemoryStream }

  TRefMemoryStream = class(TReferencedStream)
  public
    constructor Create;
    constructor Create(Sz : PtrInt); overload;
    procedure WriteTo(Strm : TStream; from, Sz : PtrInt); override;
  end;

implementation

{ TThreadGetSetValue }

procedure TThreadGetSetValue.SetValue(const AValue : T);
begin
  Lock;
  try
    FValue := AValue;
  finally
    UnLock;
  end;
end;

{ TThreadGetValue }

function TThreadGetValue.GetValue : T;
begin
  Lock;
  try
    Result := FValue;
  finally
    UnLock;
  end;
end;

constructor TThreadGetValue.Create(const AValue : T);
begin
  inherited Create;
  FValue := AValue;
end;

{ TThreadNumeric }

procedure TThreadNumeric.IncValue;
begin
  Lock;
  try
    Inc(FValue);
  finally
    UnLock;
  end;
end;

procedure TThreadNumeric.DecValue;
begin
  Lock;
  try
    Dec(FValue);
  finally
    UnLock;
  end;
end;

procedure TThreadNumeric.IncValue(IncSz : T);
begin
  Lock;
  try
    Inc(FValue, IncSz);
  finally
    UnLock;
  end;
end;

procedure TThreadNumeric.DecValue(DecSz : T);
begin
  Lock;
  try
    Dec(FValue, DecSz);
  finally
    UnLock;
  end;
end;

{ TThreadSafeFastBaseList }

function TThreadSafeFastBaseList.GetCount: integer;
begin
  Lock;
  try
    Result := FList.Count;
  finally
    UnLock;
  end;
end;

function TThreadSafeFastBaseList.GetObject(index: integer): T;
begin
  Lock;
  try
    Result := T(FList[index]);
  finally
    UnLock;
  end;
end;

procedure TThreadSafeFastBaseList.SetObject(index: integer; AValue: T);
begin
  Lock;
  try
    FList[index] := AValue;
  finally
    UnLock;
  end;
end;

procedure TThreadSafeFastBaseList.SetCount(AValue: integer);
begin
  Lock;
  try
    FList.Count := AValue;
  finally
    UnLock;
  end;
end;

constructor TThreadSafeFastBaseList.Create;
begin
  Inherited Create;
  FList := TFastList.Create;
end;

function TThreadSafeFastBaseList.Add(const Obj: T): Integer;
begin
  Lock;
  try
    Result := FList.Add(Obj);
  finally
    UnLock;
  end;
end;

function TThreadSafeFastBaseList.IndexOf(const Obj: T): Integer;
begin
  Lock;
  try
    Result := FList.IndexOf(Obj);
  finally
    UnLock;
  end;
end;

function TThreadSafeFastBaseList.Remove(const obj: T): integer;
begin
  Lock;
  try
    Result := FList.Remove(Obj);
  finally
    UnLock;
  end;
end;

procedure TThreadSafeFastBaseList.Delete(Ind: integer);
begin
  Lock;
  try
    FList.Delete(Ind);
  finally
    UnLock;
  end;
end;

procedure TThreadSafeFastBaseList.Clear;
begin
  Lock;
  try
    FList.Clear;
  finally
    UnLock;
  end;
end;

procedure TThreadSafeFastBaseList.Extract(Ind: integer);
begin
  Lock;
  try
    FList.Extract(Ind);
  finally
    UnLock;
  end;
end;

procedure TThreadSafeFastBaseList.Pack;
begin
  Lock;
  try
    FList.Pack;
  finally
    UnLock;
  end;
end;

destructor TThreadSafeFastBaseList.Destroy;
begin
  FList.Free;
  inherited Destroy;
end;

procedure TThreadSafeFastBaseList.SortList(func: TObjectSortFunction);
begin
  Lock;
  try
    FList.SortList(func);
  finally
    UnLock;
  end;
end;

{ TRefMemoryStream }

constructor TRefMemoryStream.Create;
begin
  inherited Create(TExtMemoryStream.Create);
end;

constructor TRefMemoryStream.Create(Sz : PtrInt);
begin
  inherited Create(TExtMemoryStream.Create(Sz));
end;

procedure TRefMemoryStream.WriteTo(Strm : TStream; from, Sz : PtrInt);
begin
  Strm.Write(PByte(TExtMemoryStream(Stream).Memory)[from], Sz);
end;

{ TReferencedStream }

constructor TReferencedStream.Create(aStrm: TStream);
begin
  inherited Create;
  FStream := aStrm;
end;

destructor TReferencedStream.Destroy;
begin
  FStream.Free;
  inherited Destroy;
end;

procedure TReferencedStream.WriteTo(Strm : TStream; from, Sz : PtrInt);
begin
  Lock;
  try
    FStream.Position := from;
    Strm.CopyFrom(FStream, Sz);
  finally
    UnLock;
  end;
end;

{ TThreadPointer }

constructor TThreadPointer.Create(ASize: QWord);
begin
  Inherited Create(nil);
  FValue:= GetMem(ASize);
end;

destructor TThreadPointer.Destroy;
begin
  if Assigned(FValue) then
    FreeMem(FValue);
  inherited Destroy;
end;

procedure TThreadPointer.Realloc(AValue: Pointer);
begin
  Lock;
  try
    FValue:= AValue;
  finally
    UnLock;
  end;
end;

{ TNetHandle }

procedure TNetHandle.Disconnect;
begin
  DecReference;
end;

{ THandlesList }

function THandlesList.GetCount: integer;
begin
  Result := FList.Count;
end;

function THandlesList.GetItem(index : integer): TNetHandle;
begin
  Result := TNetHandle(FList[index]);
end;

constructor THandlesList.Create;
begin
  inherited Create;
  FList := TNetReferenceHolderList.Create;
end;

destructor THandlesList.Destroy;
begin
  Clear;
  FList.Free;
  inherited Destroy;
end;

function THandlesList.GetByHandle(aHandle: Cardinal): TObject;
var
  i : integer;
begin
  Result := nil;
  FList.Lock;
  try
    for i := 0 to FList.Count-1 do
    begin
      if TNetHandle(FList[i]).Handle = aHandle then Exit(FList[i]);
    end;
  finally
    FList.UnLock;
  end;
end;

procedure THandlesList.RemoveHandle(obj: TNetHandle);
begin
  FList.Remove(obj);
end;

procedure THandlesList.AddHandle(Obj: TNetHandle);
begin
  FList.Add(obj);
end;

procedure THandlesList.Clear;
begin
  FList.Clear;
end;

{ TThreadSafeFastBaseCollection }

function TThreadSafeFastBaseCollection.GetCount: integer;
begin
  Lock;
  try
    Result := FCollection.Count;
  finally
    UnLock;
  end;
end;

function TThreadSafeFastBaseCollection.GetObject(index : integer): T;
begin
  Lock;
  try
    Result := T(FCollection[index]);
  finally
    UnLock;
  end;
end;

procedure TThreadSafeFastBaseCollection.SetCount(AValue: integer);
begin
  Lock;
  try
    FCollection.Count:=AValue;
  finally
    UnLock;
  end;
end;

procedure TThreadSafeFastBaseCollection.SetObject(index: integer; AValue: T);
begin
  Lock;
  try
    FCollection[index] := AValue;
  finally
    UnLock;
  end;
end;

constructor TThreadSafeFastBaseCollection.Create;
begin
  inherited Create;
  FCollection := TFastCollection.Create;
end;

function TThreadSafeFastBaseCollection.Add(const Obj: T): Integer;
begin
  Lock;
  try
    Result := FCollection.Add(Obj);
  finally
    UnLock;
  end;
end;

function TThreadSafeFastBaseCollection.IndexOf(const Obj: T): Integer;
begin
  Lock;
  try
    Result := FCollection.IndexOf(Obj);
  finally
    UnLock;
  end;
end;

function TThreadSafeFastBaseCollection.Remove(const obj: T): integer;
begin
  Lock;
  try
    Result := FCollection.Remove(obj);
  finally
    UnLock;
  end;
end;

procedure TThreadSafeFastBaseCollection.Delete(Ind: integer);
begin
  Lock;
  try
    FCollection.Delete(Ind);
  finally
    UnLock;
  end;
end;

procedure TThreadSafeFastBaseCollection.DeleteFreeAssigned(Ind : integer);
begin
  Lock;
  try
    FCollection.DeleteFreeAssigned(Ind);
  finally
    UnLock;
  end;
end;

procedure TThreadSafeFastBaseCollection.Clear;
begin
  Lock;
  try
    FCollection.Clear;
  finally
    UnLock;
  end;
end;

procedure TThreadSafeFastBaseCollection.Extract(Ind: integer);
begin
  Lock;
  try
    FCollection.Extract(ind);
  finally
    UnLock;
  end;
end;

procedure TThreadSafeFastBaseCollection.Pack;
begin
  Lock;
  try
    FCollection.Pack;
  finally
    UnLock;
  end;
end;

destructor TThreadSafeFastBaseCollection.Destroy;
begin
  FCollection.Free;
  inherited Destroy;
end;

procedure TThreadSafeFastBaseCollection.SortList(func: TObjectSortFunction);
begin
  Lock;
  try
    FCollection.SortList(func);
  finally
    UnLock;
  end;
end;

{ TNetReferenceHolderList }

function TNetReferenceHolderList.GetItem(index : integer): TNetReferencedObject;
begin
  Lock;
  try
    Result := TNetReferencedObject(FList[index]);
  finally
    UnLock;
  end;
end;

function TNetReferenceHolderList.GetCount: integer;
begin
  Lock;
  try
    Result := FList.Count;
  finally
    UnLock;
  end;
end;

constructor TNetReferenceHolderList.Create;
begin
  inherited Create;
  FList := TFastList.Create;
end;

destructor TNetReferenceHolderList.Destroy;
begin
  Clear;
  FList.Free;
  inherited Destroy;
end;

procedure TNetReferenceHolderList.Clear;
var
  i : integer;
begin
  Lock;
  try
    for i := 0 to FList.Count-1 do
    begin
      if assigned(FOnDisconnect) then FOnDisconnect(TNetReferencedObject(FList[i]));
      TNetReferencedObject(FList[i]).DecReference;
      FList[i] := nil;
    end;
    FList.Clear;
  finally
    UnLock;
  end;
end;

procedure TNetReferenceHolderList.Add(Obj: TNetReferencedObject);
begin
  Lock;
  try
    FList.Add(Obj);
  finally
    UnLock;
  end;
end;

procedure TNetReferenceHolderList.Remove(Obj: TNetReferencedObject);
var i : integer;
begin
  Lock;
  try
    i := FList.IndexOf(Obj);
    if i >= 0 then
    begin
      if assigned(FOnDisconnect) then FOnDisconnect(TNetReferencedObject(FList[i]));
      TNetReferencedObject(FList[i]).DecReference;
      FList.Delete(i);
    end;
  finally
    UnLock;
  end;
end;

procedure TNetReferenceHolderList.Delete(i: integer);
begin
  Lock;
  try
    if assigned(FOnDisconnect) then FOnDisconnect(TNetReferencedObject(FList[i]));
    TNetReferencedObject(FList[i]).DecReference;
    FList.Delete(i);
  finally
    UnLock;
  end;
end;

function TNetReferenceHolderList.IndexOf(Obj: TNetReferencedObject): integer;
begin
  Lock;
  try
    Result := Flist.IndexOf(Obj);
  finally
    UnLock;
  end;
end;

{ TThreadSafeFastBaseSeq }

function TThreadSafeFastBaseSeq.GetCount: integer;
begin
  Lock;
  try
    Result := FSeq.Count;
  finally
    UnLock;
  end;
end;

constructor TThreadSafeFastBaseSeq.Create;
begin
  inherited Create;
  fSeq := TFastSeq.Create;
end;

destructor TThreadSafeFastBaseSeq.Destroy;
begin
  fseq.free;
  inherited Destroy;
end;

procedure TThreadSafeFastBaseSeq.Clean;
begin
  Lock;
  try
    fSeq.Clean;
  finally
    UnLock;
  end;
end;

procedure TThreadSafeFastBaseSeq.ExtractAll;
begin
  Lock;
  try
    FSeq.ExtractAll;
  finally
    UnLock;
  end;
end;

procedure TThreadSafeFastBaseSeq.Push_back(const O: TObject);
begin
  Lock;
  try
    FSeq.Push_back(O);
  finally
    UnLock;
  end;
end;

procedure TThreadSafeFastBaseSeq.Push_front(const O: TObject);
begin
  Lock;
  try
   FSeq.Push_front(O);
  finally
    UnLock;
  end;
end;

function TThreadSafeFastBaseSeq.Pop: TIteratorObject;
begin
  Lock;
  try
    Result := FSeq.Pop;
  finally
    UnLock;
  end;
end;

function TThreadSafeFastBaseSeq.PopValue: T;
begin
  Lock;
  try
    Result := T(FSeq.PopValue);
  finally
    UnLock;
  end;
end;

function TThreadSafeFastBaseSeq.LastValue : T;
begin
  Lock;
  try
    Result := T(FSeq.LastValue);
  finally
    UnLock;
  end;
end;

function TThreadSafeFastBaseSeq.FirstValue : T;
begin
  Lock;
  try
    Result := T(FSeq.FirstValue);
  finally
    UnLock;
  end;
end;

function TThreadSafeFastBaseSeq.InsertBefore(loc: TIteratorObject; o: TObject
  ): TIteratorObject;
begin
  Lock;
  try
    Result := FSeq.InsertBefore(loc, o);
  finally
    UnLock;
  end;
end;

function TThreadSafeFastBaseSeq.InsertAfter(loc: TIteratorObject; o: TObject
  ): TIteratorObject;
begin
  Lock;
  try
    Result := FSeq.InsertAfter(loc, o);
  finally
    UnLock;
  end;
end;

procedure TThreadSafeFastBaseSeq.Erase(const loc: TIteratorObject);
begin
  Lock;
  try
    FSeq.Erase(loc);
  finally
    UnLock;
  end;
end;

procedure TThreadSafeFastBaseSeq.EraseObject(const obj: TObject);
begin
  Lock;
  try
    FSeq.EraseObject(obj);
  finally
    UnLock;
  end;
end;

function TThreadSafeFastBaseSeq.EraseObjectsByCriteria(
  criteria : TFindObjectCriteria; data : pointer) : Boolean;
begin
  Lock;
  try
    Result := FSeq.EraseObjectsByCriteria(criteria, data);
  finally
    UnLock;
  end;
end;

function TThreadSafeFastBaseSeq.FindValue(criteria : TFindObjectCriteria;
  data : Pointer) : T;
begin
  Lock;
  try
    Result := T(FSeq.FindValue(criteria, data));
  finally
    UnLock;
  end;
end;

procedure TThreadSafeFastBaseSeq.DoForAll(action : TObjectAction);
begin
  Lock;
  try
    FSeq.DoForAll(action);
  finally
    UnLock;
  end;
end;

procedure TThreadSafeFastBaseSeq.DoForAllEx(action : TExObjectAction;
  data : Pointer);
begin
  Lock;
  try
    FSeq.DoForAllEx(action, data);
  finally
    UnLock;
  end;
end;

procedure TThreadSafeFastBaseSeq.Extract(const loc: TIteratorObject);
begin
  Lock;
  try
    FSeq.Extract(loc);
  finally
    UnLock;
  end;
end;

procedure TThreadSafeFastBaseSeq.ExtractObject(const obj: TObject);
begin
  Lock;
  try
    FSeq.ExtractObject(obj);
  finally
    UnLock;
  end;
end;

function TThreadSafeFastBaseSeq.ExtractObjectsByCriteria(
  criteria : TFindObjectCriteria; afterextract : TObjectAction;
  data : pointer) : Boolean;
begin
  Lock;
  try
    Result := FSeq.ExtractObjectsByCriteria(criteria, afterextract, data);
  finally
    UnLock;
  end;
end;

function TThreadSafeFastBaseSeq.ListBegin: TIteratorObject;
begin
  Lock;
  try
    Result := FSeq.ListBegin;
  finally
    UnLock;
  end;
end;

function TThreadSafeFastBaseSeq.IteratorBegin: TIterator;
begin
  Lock;
  try
    Result := FSeq.IteratorBegin;
  finally
    UnLock;
  end;
end;

class function TThreadSafeFastBaseSeq.ListEnd: TIteratorObject;
begin
  Result := nil;
end;

{ TNetReferenceList }

destructor TNetReferenceList.Destroy;
begin
  while Count > 0 do
  begin
    CleanDead;
    Sleep(0);
  end;
  inherited Destroy;
end;

procedure TNetReferenceList.Add(const O: TNetReferencedObject);
begin
  Push_back(O);
end;

procedure TNetReferenceList.CleanDead;
begin
  EraseObjectsByCriteria(@IfNoReferences, nil);
end;

function TNetReferenceList.IfNoReferences(obj : Tobject; ptr : pointer
  ) : Boolean;
begin
  Result := TNetReferencedObject(obj).mReferenceCount <= 0;
end;

{ TNetReferencedObject }

constructor TNetReferencedObject.Create;
begin
  inherited Create;
  mReferenceCount:= 1;
end;

procedure TNetReferencedObject.IncReference;
begin
  Lock;
  try
    Inc(mReferenceCount);
  finally
    UnLock;
  end;
end;

procedure TNetReferencedObject.DecReference;
begin
  Lock;
  try
    Dec(mReferenceCount);
  finally
    UnLock;
  end;
end;

function TNetReferencedObject.HasReferences: Boolean;
begin
  Lock;
  try
    Result := mReferenceCount > 0;
  finally
    UnLock;
  end;
end;

function TNetReferencedObject.HasExternReferences: Boolean;
begin
  Lock;
  try
    Result := mReferenceCount > 1;
  finally
    UnLock;
  end;
end;

{ TNetCustomLockedObject }

constructor TNetCustomLockedObject.Create;
begin
  InitCriticalSection(FRTI);
end;

destructor TNetCustomLockedObject.Destroy;
begin
  DoneCriticalsection(FRTI);
  inherited Destroy;
end;

procedure TNetCustomLockedObject.Lock;
begin
  EnterCriticalsection(FRTI);
end;

procedure TNetCustomLockedObject.UnLock;
begin
  LeaveCriticalsection(FRTI);
end;

{ TThreadSafeAutoIncrementCardinal }

function TThreadSafeAutoIncrementCardinal.GetID: Cardinal;
begin
  Lock;
  try
    Result := FValue;
    Inc(FValue);
  finally
    UnLock;
  end;
end;

constructor TThreadSafeAutoIncrementCardinal.Create;
begin
  inherited Create;
  Reset;
end;

procedure TThreadSafeAutoIncrementCardinal.Reset;
begin
  Lock;
  try
    FValue:=1;
  finally
    UnLock;
  end;
end;

procedure TThreadSafeAutoIncrementCardinal.SetValue(v: Cardinal);
begin
  Lock;
  try
    FValue := v;
  finally
    UnLock;
  end;
end;

{ TThreadBoolean }

procedure TThreadBoolean.Enable;
begin
  Value := true;
end;

procedure TThreadBoolean.Disable;
begin
  Value := false;
end;

{ TThreadStringList }

function TThreadStringList.GetCount: Integer;
begin
  Lock;
  try
    Result := FStringList.Count;
  finally
    UnLock;
  end;
end;

function TThreadStringList.GetDelimitedText : String;
begin
  Lock;
  try
    Result := FStringList.DelimitedText;
  finally
    UnLock;
  end;
end;

function TThreadStringList.GetDelimiter : Char;
begin
  Lock;
  try
    Result := FStringList.Delimiter;
  finally
    UnLock;
  end;
end;

function TThreadStringList.GetStr(index : integer): String;
begin
  Lock;
  try
    Result := FStringList[index];
  finally
    UnLock;
  end;
end;

function TThreadStringList.GetText: String;
begin
  Lock;
  try
    Result := FStringList.Text;
  finally
    UnLock;
  end;
end;

procedure TThreadStringList.SetDelimitedText(const AValue : String);
begin
  Lock;
  try
    FStringList.DelimitedText := AValue;
  finally
    UnLock;
  end;
  DoChange;
end;

procedure TThreadStringList.SetDelimiter(AValue : Char);
begin
  Lock;
  try
    FStringList.Delimiter := AValue;
  finally
    UnLock;
  end;
end;

procedure TThreadStringList.SetStr(index : integer; const AValue: String);
begin
  Lock;
  try
    FStringList[index] := AValue;
  finally
    UnLock;
  end;
  DoChange;
end;

procedure TThreadStringList.SetText(const AValue : String);
begin
  Lock;
  try
    FStringList.Text:= AValue;
  finally
    UnLock;
  end;
  DoChange;
end;

procedure TThreadStringList.DoChange;
begin
  if FUpdates = 0 then
   if assigned(FOnChange) then
     FOnChange(Self);
end;

function TThreadStringList.GetOnChange: TNotifyEvent;
begin
  Lock;
  try
    Result := FOnChange;
  finally
    UnLock;
  end;
end;

procedure TThreadStringList.SetOnChange(AValue: TNotifyEvent);
begin
  Lock;
  try
    FOnChange:=AValue;
  finally
    UnLock;
  end;
end;

constructor TThreadStringList.Create;
begin
  inherited Create;
  FStringList := TStringList.Create;
  FUpdates := 0;
end;

destructor TThreadStringList.Destroy;
begin
  FStringList.Free;
  inherited Destroy;
end;

function TThreadStringList.Add(const S: string): Integer;
begin
  Lock;
  try
    Result := FStringList.Add(S);
  finally
    UnLock;
  end;
  DoChange;
end;

procedure TThreadStringList.Delete(Index : Integer);
begin
  Lock;
  try
    FStringList.Delete(Index);
  finally
    UnLock;
  end;
  DoChange;
end;

procedure TThreadStringList.DeleteFromTo(Index1, Index2 : Integer);
var i : integer;
begin
  Lock;
  try
    i := Index1;
    while i <= Index2 do
    begin
      FStringList.Delete(Index1);
      Inc(I);
    end;
  finally
    UnLock;
  end;
  DoChange;
end;

function TThreadStringList.IndexOf(const S: string): Integer;
begin
  Lock;
  try
    Result := FStringList.IndexOf(S);
  finally
    UnLock;
  end;
end;

procedure TThreadStringList.AddStrings(S: TStringList);
begin
  Lock;
  try
    FStringList.AddStrings(S);
  finally
    UnLock;
  end;
  DoChange;
end;

procedure TThreadStringList.BeginUpdate;
begin
  Lock;
  FStringList.BeginUpdate;
  Inc(FUpdates);
end;

procedure TThreadStringList.EndUpdate;
begin
  FStringList.EndUpdate;
  Dec(FUpdates);
  UnLock;
  DoChange;
end;

procedure TThreadStringList.Clear;
begin
  if FStringList.Count > 0 then
  begin
    Lock;
    try
      FStringList.Clear;
    finally
      UnLock;
    end;
    DoChange;
  end;
end;

end.


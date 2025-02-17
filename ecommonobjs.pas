{
 ECommonObjs:
   Thread safe helpers to access to objects and data

   Part of ESolver project

   Copyright (c) 2019-2024 by Ilya Medvedkov

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
}

unit ECommonObjs;

{$mode objfpc}{$H+}

{.$define AtomicAsRW}
{$define AtomicAsInterlocked}
{$define UseLibInterlockedOps}
{.$define AtomicAsCS}
{$ifdef AtomicAsRW}
{$define userwlocks}
{$else}
{.$define userwlocks}
{$endif}

{$IF defined(CPUX86_64) or defined(CPUX64)}
  {$DEFINE x64}
{$ELSEIF defined(CPU386)}
  {$DEFINE x86}
{$ELSE}
  {$undef UseLibInterlockedOps}
{$IFEND}

interface

uses
  Classes, SysUtils, OGLFastList, extmemorystream
  {$ifdef userwlocks}
  {$ifdef Windows}
  ,Windows
  {$else}
   {$ifdef Unix}
    {$ifdef usecthreads}
     ,cthreads
    {$endif}
    ,UnixType,PThreads
   {$endif}
  {$endif}
  {$endif};

type

  { TNetInterlocked }

  TNetInterlocked=class
  public
    class function Increment(var Destination:Int32):Int32; overload; static; inline;
    class function Increment(var Destination:UInt32):UInt32; overload; static; inline;
    class function Increment(var Destination:Int64):Int64; overload; static; inline;
    class function Increment(var Destination:UInt64):UInt64; overload; static; inline;
    class function Decrement(var Destination:Int32):Int32; overload; static; inline;
    class function Decrement(var Destination:UInt32):UInt32; overload; static; inline;
    class function Decrement(var Destination:Int64):Int64; overload; static; inline;
    class function Decrement(var Destination:UInt64):UInt64; overload; static; inline;
    class function Add(var Destination:Int32;const Value:Int32):Int32; overload; static; inline;
    class function Add(var Destination:UInt32;const Value:UInt32):UInt32; overload; static; inline;
    class function Add(var Destination:Int64;const Value:Int64):Int64; overload; static; inline;
    class function Add(var Destination:UInt64;const Value:UInt64):UInt64; overload; static; inline;
    class function Sub(var Destination:Int32;const Value:Int32):Int32; overload; static; inline;
    class function Sub(var Destination:UInt32;const Value:UInt32):UInt32; overload; static; inline;
    class function Sub(var Destination:Int64;const Value:Int64):Int64; overload; static; inline;
    class function Sub(var Destination:UInt64;const Value:UInt64):UInt64; overload; static; inline;
    class function Exchange(var Destination:Int32;const Source:Int32):Int32; overload; static; inline;
    class function Exchange(var Destination:UInt32;const Source:UInt32):UInt32; overload; static; inline;
    class function Exchange(var Destination:Int64;const Source:Int64):Int64; overload; static; inline;
    class function Exchange(var Destination:UInt64;const Source:UInt64):UInt64; overload; static; inline;
    class function Exchange(var Destination:pointer;const Source:pointer):pointer; overload; static; inline;
    class function Exchange(var Destination:TObject;const Source:TObject):TObject; overload; static; inline;
    class function Exchange(var Destination:LongBool;const Source:LongBool):LongBool; overload; static; inline;
    class function CompareExchange(var Destination:Int32;const NewValue,Comperand:Int32):Int32; overload; static; inline;
    class function CompareExchange(var Destination:UInt32;const NewValue,Comperand:UInt32):UInt32; overload; static; inline;
    class function CompareExchange(var Destination:Int64;const NewValue,Comperand:Int64):Int64; overload; static; inline;
    class function CompareExchange(var Destination:UInt64;const NewValue,Comperand:UInt64):UInt64; overload; static; inline;
    class function CompareExchange(var Destination:pointer;const NewValue,Comperand:pointer):pointer; overload; static; inline;
    class function CompareExchange(var Destination:TObject;const NewValue,Comperand:TObject):TObject; overload; static; inline;
    class function CompareExchange(var Destination:LongBool;const NewValue,Comperand:LongBool):LongBool; overload; static; inline;
    class function Read(var Source:Int32):Int32; overload; static; inline;
    class function Read(var Source:UInt32):UInt32; overload; static; inline;
    class function Read(var Source:Int64):Int64; overload; static; inline;
    class function Read(var Source:UInt64):UInt64; overload; static; inline;
    class function Read(var Source:pointer):pointer; overload; static; inline;
    class function Read(var Source:TObject):TObject; overload; static; inline;
    class function Read(var Source:LongBool):LongBool; overload; static; inline;
    class function Write(var Destination:Int32;const Source:Int32):Int32; overload; static; inline;
    class function Write(var Destination:UInt32;const Source:UInt32):UInt32; overload; static; inline;
    class function Write(var Destination:Int64;const Source:Int64):Int64; overload; static; inline;
    class function Write(var Destination:UInt64;const Source:UInt64):UInt64; overload; static; inline;
    class function Write(var Destination:pointer;const Source:pointer):pointer; overload; static; inline;
    class function Write(var Destination:TObject;const Source:TObject):TObject; overload; static; inline;
    class function Write(var Destination:LongBool;const Source:LongBool):LongBool; overload; static; inline;
    {$ifdef UseLibInterlockedOps}
    class function InterlockedAnd(var A: QWord; B: QWord): QWord; overload; static; inline;
    class function InterlockedAnd(var A: Int64; B: Int64): Int64; overload; static; inline;
    class function InterlockedAnd(var A: Int32; B: Int32): Int32; overload; static; inline;
    class function InterlockedAnd(var A: Cardinal; B: Cardinal): Cardinal; overload; static; inline;
    class function InterlockedAnd(var A: LongBool; B: LongBool): LongBool; overload; static; inline;
    class function InterlockedOr(var A: QWord; B: QWord): QWord; overload; static; inline;
    class function InterlockedOr(var A: Int64; B: Int64): Int64; overload; static; inline;
    class function InterlockedOr(var A: Int32; B: Int32): Int32; overload; static; inline;
    class function InterlockedOr(var A: Cardinal; B: Cardinal): Cardinal; overload; static; inline;
    class function InterlockedOr(var A: LongBool; B: LongBool): LongBool; overload; static; inline;
    class function InterlockedXor(var A: QWord; B: QWord): QWord; overload; static; inline;
    class function InterlockedXor(var A: Int64; B: Int64): Int64; overload; static; inline;
    class function InterlockedXor(var A: Int32; B: Int32): Int32; overload; static; inline;
    class function InterlockedXor(var A: Cardinal; B: Cardinal): Cardinal; overload; static; inline;
    class function InterlockedXor(var A: LongBool; B: LongBool): LongBool; overload; static; inline;
    class function InterlockedNot(var A: QWord): QWord; overload; static; inline;
    class function InterlockedNot(var A: Int64): Int64; overload; static; inline;
    class function InterlockedNot(var A: Int32): Int32; overload; static; inline;
    class function InterlockedNot(var A: Cardinal): Cardinal; overload; static; inline;
    class function InterlockedNot(var A: LongBool): LongBool; overload; static; inline;
    class function InterlockedShr(var A: QWord; Cnt : Integer): QWord; overload; static; inline;
    class function InterlockedShr(var A: Int64; Cnt : Integer): Int64; overload; static; inline;
    class function InterlockedShr(var A: Int32; Cnt : Integer): Int32; overload; static; inline;
    class function InterlockedShr(var A: Cardinal; Cnt : Integer): Cardinal; overload; static; inline;
    class function InterlockedShr(var A: LongBool; Cnt : Integer): LongBool; overload; static; inline;
    class function InterlockedShl(var A: QWord; Cnt : Integer): QWord; overload; static; inline;
    class function InterlockedShl(var A: Int64; Cnt : Integer): Int64; overload; static; inline;
    class function InterlockedShl(var A: Int32; Cnt : Integer): Int32; overload; static; inline;
    class function InterlockedShl(var A: Cardinal; Cnt : Integer): Cardinal; overload; static; inline;
    class function InterlockedShl(var A: LongBool; Cnt : Integer): LongBool; overload; static; inline;
    {$endif}
  end;

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

  {$ifdef userwlocks}

  { TNetRWVariable }

  TNetRWVariable = class
  {$if defined(Windows)}
  private
    fSRWLock:TPasMPSRWLock;
  {$elseif defined(Unix)}
  private
    fReadWriteLock:pthread_rwlock_t;
  {$ifend}
  public
    constructor Create;
    destructor Destroy; override;

    procedure BeginRead;
    procedure EndRead;
    procedure BeginWrite;
    procedure EndWrite;
  end;

  {$endif}

  { TAtomicVariable }

  generic TAtomicVariable<T{$ifdef AtomicAsInterlocked},ST{$endif}> = class(
  {$ifdef AtomicAsRW}
  TNetRWVariable
  {$else}
  {$ifdef AtomicAsInterlocked}
  TNetInterlocked
  {$else}
  TNetCustomLockedObject
  {$endif}
  {$endif})
  private
    FValue : {$ifdef AtomicAsInterlocked}ST{$else}T{$endif};
    function GetValue: T;
    procedure SetValue(AValue: T);
  public
    constructor Create(aValue : T);

    property Value : T read GetValue write SetValue;
  end;

  TAtomicBoolean = class(specialize TAtomicVariable<Boolean{$ifdef AtomicAsInterlocked},LongBool{$endif}>);
  TAtomicPointer = class(specialize TAtomicVariable<Pointer{$ifdef AtomicAsInterlocked},Pointer{$endif}>);

  { TAtomicNumeric }

  generic TAtomicNumeric<T{$ifdef AtomicAsInterlocked},ST{$endif}> = class(specialize TAtomicVariable<T{$ifdef AtomicAsInterlocked},ST{$endif}>)
  public
    function AddValue(const aValue : T) : T;
    function SubValue(const aValue : T) : T;
    function AndValue(const aValue : T) : T;
    function OrValue(const aValue : T) : T;
    function XorValue(const aValue : T) : T;
    function ShlValue(aCount : Integer) : T;
    function ShrValue(aCount : Integer) : T;
    function NotValue() : T;

    procedure IncValue;
    procedure DecValue;
  end;

  TAtomicInteger = class(specialize TAtomicNumeric<Integer{$ifdef AtomicAsInterlocked},Int32{$endif}>);
  TAtomicCardinal = class(specialize TAtomicNumeric<Cardinal{$ifdef AtomicAsInterlocked},UInt32{$endif}>);
  TAtomicInt64 = class(specialize TAtomicNumeric<Int64{$ifdef AtomicAsInterlocked},Int64{$endif}>);
  TAtomicQWord = class(specialize TAtomicNumeric<QWord{$ifdef AtomicAsInterlocked},Uint64{$endif}>);

  { TNetReferencedObject }

  TNetReferencedObject = class(TNetCustomLockedObject)
  private
    mReferenceCount : TAtomicInteger;
  public
    constructor Create;
    destructor Destroy; override;
    procedure IncReference;
    procedure DecReference; virtual;
    function HasReferences : Boolean;
    function HasExternReferences : Boolean;
    function NoReferences: Boolean;
  end;

  TNetAutoRefRemoved = procedure (obj : TNetReferencedObject) of object;

  { TNetAutoReferencedObject }

  TNetAutoReferencedObject = class(TNetReferencedObject)
  private
    FOnRemove : TNetAutoRefRemoved;
  public
    procedure DecReference; override;

    property OnRemove : TNetAutoRefRemoved read FOnRemove write FOnRemove;
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
    procedure SetValue(const AValue: T); virtual;
  public
    property Value : T read GetValue write SetValue;
  end;

  { TThreadNumeric }

  generic TThreadNumeric<T> = class(specialize TThreadGetSetValue<T>)
  public
    procedure IncValue; virtual; overload;
    procedure DecValue; virtual; overload;
    procedure IncValue(IncSz : T); virtual; overload;
    procedure DecValue(DecSz: T); virtual; overload;
    procedure OrValue(mask : T); virtual;
    procedure AndValue(mask : T); virtual;
    procedure XorValue(mask : T); virtual;
    procedure NotValue; virtual;

    function BitwiseOr(mask : T) : T;
    function BitwiseAnd(mask : T) : T;
    function BitwiseXor(mask : T) : T;
  end;

  { TThreadActiveNumeric }

  generic TThreadActiveNumeric<T> = class(specialize TThreadNumeric<T>)
  private
    FOnChange : TNotifyEvent;
    procedure DoOnChange;
    procedure SetValue(const AValue: T); override;
  public
    procedure IncValue; override; overload;
    procedure DecValue; override; overload;
    procedure IncValue(IncSz : T); override; overload;
    procedure DecValue(DecSz: T); override; overload;
    procedure OrValue(mask : T); override;
    procedure AndValue(mask : T); override;
    procedure XorValue(mask : T); override;
    procedure NotValue; override;
    property OnChange : TNotifyEvent read FOnChange write FOnChange;
  end;

  TThreadByte = class(specialize TThreadNumeric<Byte>);
  TThreadWord = class(specialize TThreadNumeric<Word>);
  TThreadInteger = class(specialize TThreadNumeric<Integer>);
  TThreadCardinal = class(specialize TThreadNumeric<Cardinal>);
  TThreadQWord = class(specialize TThreadNumeric<QWord>);
  TThreadInt64 = class(specialize TThreadNumeric<Int64>);

  TThreadActiveByte = class(specialize TThreadActiveNumeric<Byte>);
  TThreadActiveWord = class(specialize TThreadActiveNumeric<Word>);
  TThreadActiveInteger = class(specialize TThreadActiveNumeric<Integer>);
  TThreadActiveCardinal = class(specialize TThreadActiveNumeric<Cardinal>);
  TThreadActiveQWord = class(specialize TThreadActiveNumeric<QWord>);
  TThreadActiveInt64 = class(specialize TThreadActiveNumeric<Int64>);

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
    procedure SaveToFile(const FN : String);
    function IndexOf(const S: string): Integer;
    procedure AddStrings(S : TStringList);
    procedure BeginUpdate;
    procedure EndUpdate;
    procedure Clear;
    procedure ConcatText(const aText : String);
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

  { TThreadActiveFastBaseCollection }

  generic TThreadActiveFastBaseCollection<T> = class(specialize TThreadSafeFastBaseCollection<T>)
  private
    FOnChange : TNotifyEvent;
    FUpdateCnt : integer;
    procedure DoChange;
    function GetOnChange: TNotifyEvent;
    procedure SetOnChange(AValue: TNotifyEvent);
  protected
    procedure SetCount(AValue: integer); override;
  public
    constructor Create;
    procedure BeginUpdate;
    procedure EndUpdate;
    function Add(const Obj : T) : Integer; override;
    function Remove(const obj: T) : integer; override;
    procedure Delete(Ind : integer); override;
    procedure Clear; override;
    procedure Extract(Ind : integer); override;
    procedure Pack; override;
    property OnChange : TNotifyEvent read GetOnChange write SetOnChange;
  end;

  { TThreadActiveFastBaseSeq }

  generic TThreadActiveFastBaseSeq<T> = class(specialize TThreadSafeFastBaseSeq<T>)
  private
    FOnChange : TNotifyEvent;
    FUpdateCnt : integer;
    procedure DoChange;
    function GetOnChange: TNotifyEvent;
    procedure SetOnChange(AValue: TNotifyEvent);
  public
    constructor Create;
    procedure BeginUpdate;
    procedure EndUpdate;
    procedure Push_back(const O : TObject); override;
    procedure Push_front(const O : TObject); override;
    function InsertBefore(loc: TIteratorObject; o: TObject): TIteratorObject; override;
    function InsertAfter(loc: TIteratorObject; o: TObject): TIteratorObject; override;

    property OnChange : TNotifyEvent read GetOnChange write SetOnChange;
  end;

  { TNetAutoReferenceList }

  TNetAutoReferenceList = class
  private
    FRefCnt : TAtomicInteger;
    function GetCount : Integer;
    procedure UnRegister(v : TNetReferencedObject);
  public
    constructor Create;
    destructor Destroy; override;
    procedure Add(const O : TNetAutoReferencedObject);
    procedure CleanDead;

    property Count : Integer read GetCount;
  end;

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

  TReferencedStream = class(TNetAutoReferencedObject)
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

{$ifdef USELIBINTERLOCKEDOPS}

{$ASMMODE Intel}
{$IFDEF x64}
  {$DEFINE Ptr64}
{$ELSE}
  {$IF SizeOf(Pointer) <> 4 }
  {$MESSAGE FATAL 'Unsupported size of pointers.'}
  {$ENDIF}
{$ENDIF}

{
   This code block based on InterlockedOps library  Version 1.4.2 (2022-02-21)
   git@github.com:iLya2IK/lib.InterlockedOps.git
   ©2021-2024 Frantisek Milt
   Contacts:
     Frantisek Milt: frantisek.milt@gmail.com
}

//------------------------------------------------------------------------------

Function InterlockedLoad32(I: Pointer): UInt32;
begin
asm
          XOR   EDX, EDX
{$IFDEF x64}
  {$IFDEF Windows}
    LOCK  XADD  dword ptr [RCX], EDX
  {$ELSE}
    LOCK  XADD  dword ptr [RDI], EDX
  {$ENDIF}
{$ELSE}
    LOCK  XADD  dword ptr [EAX], EDX
{$ENDIF}
          MOV   EAX, EDX
end;
end;

//------------------------------------------------------------------------------

Function InterlockedLoad64(I: Pointer): UInt64;
begin
asm
{$IFDEF x64}

          XOR   RDX, RDX
  {$IFDEF Windows}
    LOCK  XADD  qword ptr [RCX], RDX
  {$ELSE}
    LOCK  XADD  qword ptr [RDI], RDX
  {$ENDIF}
          MOV   RAX, RDX

{$ELSE}// -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -

          PUSH  EBX
          PUSH  EDI

          MOV   EDI, EAX

    @TryOutStart:

          MOV   EAX, dword ptr [EDI]
          MOV   EDX, dword ptr [EDI + 4]

          MOV   EBX, EAX
          MOV   ECX, EDX

    LOCK  CMPXCHG8B qword ptr [EDI]

          JNZ   @TryOutStart

          POP   EDI
          POP   EBX

{$ENDIF}
end;
end;

//------------------------------------------------------------------------------

Function InterlockedLoadBool(I: Pointer): Boolean;
begin
Result := Boolean(InterlockedLoad32(I));
end;

//------------------------------------------------------------------------------

Function InterlockedStore32(I: Pointer; NewValue: UInt32): UInt32;
begin
asm
{$IFDEF x64}
  {$IFDEF Windows}

    LOCK  XCHG  dword ptr [RCX], EDX
          MOV   EAX, EDX

  {$ELSE}//  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -

    LOCK  XCHG  dword ptr [RDI], ESI
          MOV   EAX, ESI

  {$ENDIF}
{$ELSE}// -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -

    LOCK  XCHG  dword ptr [EAX], EDX
          MOV   EAX, EDX

{$ENDIF}
end;
end;

//------------------------------------------------------------------------------

Function InterlockedStore64(I: Pointer; NewValue: UInt64): UInt64;
begin
asm
{$IFDEF x64}
  {$IFDEF Windows}

    LOCK  XCHG  qword ptr [RCX], RDX
          MOV   RAX, RDX

  {$ELSE}//  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -

    LOCK  XCHG  qword ptr [RDI], RSI
          MOV   RAX, RSI

  {$ENDIF}
{$ELSE}// -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -

          PUSH  EBX
          PUSH  EDI

          MOV   EDI, EAX

    @TryOutStart:

          MOV   EBX, dword ptr [NewValue]
          MOV   ECX, dword ptr [NewValue + 4]

          MOV   EAX, dword ptr [EDI]
          MOV   EDX, dword ptr [EDI + 4]

    LOCK  CMPXCHG8B qword ptr [EDI]

          JNZ   @TryOutStart

          POP   EDI
          POP   EBX

{$ENDIF}
end;
end;

//------------------------------------------------------------------------------

Function InterlockedStoreBool(I: Pointer; NewValue: Boolean): Boolean;
begin
Result := Boolean(InterlockedStore32(I,UInt32(NewValue)));
end;

//------------------------------------------------------------------------------

Function InterlockedAnd32(A: Pointer; B: UInt32): UInt32;
begin
asm
{$IFDEF x64}

    @TryOutStart:

  {$IFDEF Windows}
          MOV   EAX, dword ptr [RCX]
  {$ELSE}
          MOV   EAX, dword ptr [RDI]
  {$ENDIF}

          MOV   R8D, EAX
          AND   R8D, B

  {$IFDEF Windows}
    LOCK  CMPXCHG dword ptr [RCX], R8D
  {$ELSE}
    LOCK  CMPXCHG dword ptr [RDI], R8D
  {$ENDIF}

          JNZ   @TryOutStart

          MOV   EAX, R8D

{$ELSE}// -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -

          PUSH  EBX

          MOV   ECX, EAX

    @TryOutStart:

          MOV   EAX, dword ptr [ECX]

          MOV   EBX, EAX
          AND   EBX, EDX

    LOCK  CMPXCHG dword ptr [ECX], EBX

          JNZ   @TryOutStart

          MOV   EAX, EBX

          POP   EBX

{$ENDIF}
end;
end;

//------------------------------------------------------------------------------

Function InterlockedAnd64(A: Pointer; B: UInt64): UInt64;
begin
asm
{$IFDEF x64}

    @TryOutStart:

  {$IFDEF Windows}
          MOV   RAX, qword ptr [RCX]
  {$ELSE}
          MOV   RAX, qword ptr [RDI]
  {$ENDIF}

          MOV   R8, RAX
          AND   R8, B

  {$IFDEF Windows}
    LOCK  CMPXCHG qword ptr [RCX], R8
  {$ELSE}
    LOCK  CMPXCHG qword ptr [RDI], R8
  {$ENDIF}

          JNZ   @TryOutStart

          MOV   RAX, R8

{$ELSE}// -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -

          PUSH  EBX
          PUSH  EDI

          MOV   EDI, EAX

    @TryOutStart:

          MOV   EAX, dword ptr [EDI]
          MOV   EDX, dword ptr [EDI + 4]

          MOV   EBX, dword ptr [B]
          MOV   ECX, dword ptr [B + 4]

          AND   EBX, EAX
          AND   ECX, EDX

    LOCK  CMPXCHG8B qword ptr [EDI]

          JNZ   @TryOutStart

          MOV   EAX, EBX
          MOV   EDX, ECX

          POP   EDI
          POP   EBX

{$ENDIF}
end;
end;

//------------------------------------------------------------------------------

Function InterlockedOr32(A: Pointer; B: UInt32): UInt32;
begin
asm
{$IFDEF x64}

    @TryOutStart:

  {$IFDEF Windows}
          MOV   EAX, dword ptr [RCX]
  {$ELSE}
          MOV   EAX, dword ptr [RDI]
  {$ENDIF}

          MOV   R8D, EAX
          OR    R8D, B

  {$IFDEF Windows}
    LOCK  CMPXCHG dword ptr [RCX], R8D
  {$ELSE}
    LOCK  CMPXCHG dword ptr [RDI], R8D
  {$ENDIF}

          JNZ   @TryOutStart

          MOV   EAX, R8D

{$ELSE}// -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -

          PUSH  EBX

          MOV   ECX, EAX

    @TryOutStart:

          MOV   EAX, dword ptr [ECX]

          MOV   EBX, EAX
          OR    EBX, EDX

    LOCK  CMPXCHG dword ptr [ECX], EBX

          JNZ   @TryOutStart

          MOV   EAX, EBX

          POP   EBX

{$ENDIF}
end;
end;

//------------------------------------------------------------------------------

Function InterlockedOr64(A: Pointer; B: UInt64): UInt64;
begin
asm
{$IFDEF x64}

    @TryOutStart:

  {$IFDEF Windows}
          MOV   RAX, qword ptr [RCX]
  {$ELSE}
          MOV   RAX, qword ptr [RDI]
  {$ENDIF}

          MOV   R8, RAX
          OR    R8, B

  {$IFDEF Windows}
    LOCK  CMPXCHG qword ptr [RCX], R8
  {$ELSE}
    LOCK  CMPXCHG qword ptr [RDI], R8
  {$ENDIF}

          JNZ   @TryOutStart

          MOV   RAX, R8

{$ELSE}// -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -

          PUSH  EBX
          PUSH  EDI

          MOV   EDI, EAX

    @TryOutStart:

          MOV   EAX, dword ptr [EDI]
          MOV   EDX, dword ptr [EDI + 4]

          MOV   EBX, dword ptr [B]
          MOV   ECX, dword ptr [B + 4]

          OR    EBX, EAX
          OR    ECX, EDX

    LOCK  CMPXCHG8B qword ptr [EDI]

          JNZ   @TryOutStart

          MOV   EAX, EBX
          MOV   EDX, ECX

          POP   EDI
          POP   EBX

{$ENDIF}
end;
end;

//------------------------------------------------------------------------------

Function InterlockedXor32(A: Pointer; B: UInt32): UInt32;
begin
asm
{$IFDEF x64}

    @TryOutStart:

  {$IFDEF Windows}
          MOV   EAX, dword ptr [RCX]
  {$ELSE}
          MOV   EAX, dword ptr [RDI]
  {$ENDIF}

          MOV   R8D, EAX
          XOR   R8D, B

  {$IFDEF Windows}
    LOCK  CMPXCHG dword ptr [RCX], R8D
  {$ELSE}
    LOCK  CMPXCHG dword ptr [RDI], R8D
  {$ENDIF}

          JNZ   @TryOutStart

          MOV   EAX, R8D

{$ELSE}// -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -

          PUSH  EBX

          MOV   ECX, EAX

    @TryOutStart:

          MOV   EAX, dword ptr [ECX]

          MOV   EBX, EAX
          XOR   EBX, EDX

    LOCK  CMPXCHG dword ptr [ECX], EBX

          JNZ   @TryOutStart

          MOV   EAX, EBX

          POP   EBX

{$ENDIF}
end;
end;

//------------------------------------------------------------------------------

Function InterlockedXor64(A: Pointer; B: UInt64): UInt64;
begin
asm
{$IFDEF x64}

    @TryOutStart:

  {$IFDEF Windows}
          MOV   RAX, qword ptr [RCX]
  {$ELSE}
          MOV   RAX, qword ptr [RDI]
  {$ENDIF}

          MOV   R8, RAX
          XOR   R8, B

  {$IFDEF Windows}
    LOCK  CMPXCHG qword ptr [RCX], R8
  {$ELSE}
    LOCK  CMPXCHG qword ptr [RDI], R8
  {$ENDIF}

          JNZ   @TryOutStart

          MOV   RAX, R8

{$ELSE}// -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -

          PUSH  EBX
          PUSH  EDI

          MOV   EDI, EAX

    @TryOutStart:

          MOV   EAX, dword ptr [EDI]
          MOV   EDX, dword ptr [EDI + 4]

          MOV   EBX, dword ptr [B]
          MOV   ECX, dword ptr [B + 4]

          XOR   EBX, EAX
          XOR   ECX, EDX

    LOCK  CMPXCHG8B qword ptr [EDI]

          JNZ   @TryOutStart

          MOV   EAX, EBX
          MOV   EDX, ECX

          POP   EDI
          POP   EBX

{$ENDIF}
end;
end;

//------------------------------------------------------------------------------

Function InterlockedShr32(A: Pointer; B: Integer): UInt32;
begin
asm
{$IFDEF x64}

    @TryOutStart:

          MOV ECX, B
  {$IFDEF Windows}
          MOV   EAX, dword ptr [RCX]
  {$ELSE}
          MOV   EAX, dword ptr [RDI]
  {$ENDIF}

          MOV   R8D, EAX
          SHR   R8D, CL

  {$IFDEF Windows}
    LOCK  CMPXCHG dword ptr [RCX], R8D
  {$ELSE}
    LOCK  CMPXCHG dword ptr [RDI], R8D
  {$ENDIF}

          JNZ   @TryOutStart

          MOV   EAX, R8D

{$ELSE}// -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -

          PUSH  EBX

          MOV   ECX, B
          MOV   EDX, EAX

    @TryOutStart:

          MOV   EAX, dword ptr [EDX]

          MOV   EBX, EAX
          SHR   EBX, CL

    LOCK  CMPXCHG dword ptr [EDX], EBX

          JNZ   @TryOutStart

          MOV   EAX, EBX

          POP   EBX

{$ENDIF}
end;
end;

//------------------------------------------------------------------------------

Function InterlockedShr64(A: Pointer; B: Integer): UInt64;
begin
asm
{$IFDEF x64}

    @TryOutStart:

          MOV   ECX, B
  {$IFDEF Windows}
          MOV   RAX, qword ptr [RCX]
  {$ELSE}
          MOV   RAX, qword ptr [RDI]
  {$ENDIF}

          MOV   R8, RAX
          SHR   R8, CL

  {$IFDEF Windows}
    LOCK  CMPXCHG qword ptr [RCX], R8
  {$ELSE}
    LOCK  CMPXCHG qword ptr [RDI], R8
  {$ENDIF}

          JNZ   @TryOutStart

          MOV   RAX, R8

{$ELSE}// -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -


          PUSH  EBX
          PUSH  EDI

          MOV   EDI, EAX
          MOV   ECX, B

     @TryOutStart:

          MOV   EAX, dword ptr [EDI]
          MOV   EDX, dword ptr [EDI + 4]

          PUSH EAX
          PUSH EDX

          CMP  CL, 64
          JAE  @RETZERO

          CMP  CL, 32
          JAE  @MORE32

          SHRD  EAX, EDX, CL
          SHR   EDX, CL

          JMP @OUTPOS

    @MORE32:
          MOV     EAX, EDX
          SHR     EDX, 31
          AND     CL,  31
          SHR     EAX, CL

          JMP @OUTPOS

    @RETZERO:
          XOR     EAX,EAX
          XOR     EDX,EDX

    @OUTPOS:
          MOV EBX, EAX
          MOV ECX, EDX

          POP EDX
          POP EAX

    LOCK  CMPXCHG8B qword ptr [EDI]

          JNZ   @TryOutStart
          MOV   EAX, EBX
          MOV   EDX, ECX

          POP   EDI
          POP   EBX

{$ENDIF}
end;
end;

//------------------------------------------------------------------------------

Function InterlockedShl32(A: Pointer; B: Integer): UInt32;
begin
asm
{$IFDEF x64}

    @TryOutStart:

          MOV ECX, B
  {$IFDEF Windows}
          MOV   EAX, dword ptr [RCX]
  {$ELSE}
          MOV   EAX, dword ptr [RDI]
  {$ENDIF}

          MOV   R8D, EAX
          SHL   R8D, CL

  {$IFDEF Windows}
    LOCK  CMPXCHG dword ptr [RCX], R8D
  {$ELSE}
    LOCK  CMPXCHG dword ptr [RDI], R8D
  {$ENDIF}

          JNZ   @TryOutStart

          MOV   EAX, R8D

{$ELSE}// -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -

          PUSH  EBX

          MOV   ECX, B
          MOV   EDX, EAX

    @TryOutStart:

          MOV   EAX, dword ptr [EDX]

          MOV   EBX, EAX
          SHL   EBX, CL

    LOCK  CMPXCHG dword ptr [EDX], EBX

          JNZ   @TryOutStart

          MOV   EAX, EBX

          POP   EBX

{$ENDIF}
end;
end;

//------------------------------------------------------------------------------

Function InterlockedShl64(A: Pointer; B: Integer): UInt64;
begin
asm
{$IFDEF x64}

    @TryOutStart:

          MOV   ECX, B
  {$IFDEF Windows}
          MOV   RAX, qword ptr [RCX]
  {$ELSE}
          MOV   RAX, qword ptr [RDI]
  {$ENDIF}

          MOV   R8, RAX
          SHL   R8, CL

  {$IFDEF Windows}
    LOCK  CMPXCHG qword ptr [RCX], R8
  {$ELSE}
    LOCK  CMPXCHG qword ptr [RDI], R8
  {$ENDIF}

          JNZ   @TryOutStart

          MOV   RAX, R8

{$ELSE}// -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -

          PUSH  EBX
          PUSH  EDI

          MOV   EDI, EAX
          MOV   ECX, B

     @TryOutStart:

          MOV   EAX, dword ptr [EDI]
          MOV   EDX, dword ptr [EDI + 4]

          PUSH EAX
          PUSH EDX

          CMP  CL,64
          JAE  @RETZERO

          CMP  CL, 32
          JAE  @MORE32

          SHLD  EDX, EAX, CL
          SHL   EAX, CL

          JMP @OUTPOS

    @MORE32:
          MOV     EDX, EAX
          XOR     EAX,EAX
          AND     CL,  31
          SHL     EDX, CL

          JMP @OUTPOS

    @RETZERO:
          XOR     EAX,EAX
          XOR     EDX,EDX

    @OUTPOS:
          MOV EBX, EAX
          MOV ECX, EDX

          POP EDX
          POP EAX

    LOCK  CMPXCHG8B qword ptr [EDI]

          JNZ   @TryOutStart
          MOV   EAX, EBX
          MOV   EDX, ECX

          POP   EDI
          POP   EBX

{$ENDIF}
end;
end;


//------------------------------------------------------------------------------

Function InterlockedNot32(I: Pointer): UInt32;
begin
asm
{$IFDEF x64}

    @TryOutStart:

  {$IFDEF Windows}
          MOV   EAX, dword ptr [RCX]
  {$ELSE}
          MOV   EAX, dword ptr [RDI]
  {$ENDIF}

          MOV   EDX, EAX
          NOT   EDX

  {$IFDEF Windows}
    LOCK  CMPXCHG dword ptr [RCX], EDX
  {$ELSE}
    LOCK  CMPXCHG dword ptr [RDI], EDX
  {$ENDIF}

          JNZ   @TryOutStart

          MOV   EAX, EDX

{$ELSE}// -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -

          MOV   ECX, EAX

    @TryOutStart:

          MOV   EAX, dword ptr [ECX]

          MOV   EDX, EAX
          NOT   EDX

    LOCK  CMPXCHG dword ptr [ECX], EDX

          JNZ   @TryOutStart

          MOV   EAX, EDX

{$ENDIF}
end;
end;

//------------------------------------------------------------------------------

Function InterlockedNot64(I: Pointer): UInt64;
begin
asm
{$IFDEF x64}

    @TryOutStart:

  {$IFDEF Windows}
          MOV   RAX, qword ptr [RCX]
  {$ELSE}
          MOV   RAX, qword ptr [RDI]
  {$ENDIF}

          MOV   RDX, RAX
          NOT   RDX

  {$IFDEF Windows}
    LOCK  CMPXCHG qword ptr [RCX], RDX
  {$ELSE}
    LOCK  CMPXCHG qword ptr [RDI], RDX
  {$ENDIF}

          JNZ   @TryOutStart

          MOV   RAX, RDX

{$ELSE}// -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -

          PUSH  EBX
          PUSH  EDI

          MOV   EDI, EAX

    @TryOutStart:

          MOV   EAX, dword ptr [EDI]
          MOV   EDX, dword ptr [EDI + 4]

          MOV   EBX, EAX
          MOV   ECX, EDX

          NOT   EBX
          NOT   ECX

    LOCK  CMPXCHG8B qword ptr [EDI]

          JNZ   @TryOutStart

          MOV   EAX, EBX
          MOV   EDX, ECX

          POP   EDI
          POP   EBX

{$ENDIF}
end;
end;

{$ifndef x64}

//------------------------------------------------------------------------------

Function InterlockedIncrement64(I: Pointer): UInt64;
begin
asm
    PUSH  EBX
    PUSH  EDI

    MOV   EDI, EAX

    @TryOutStart:

    MOV   EAX, dword ptr [EDI]
    MOV   EDX, dword ptr [EDI + 4]

    MOV   EBX, EAX
    MOV   ECX, EDX

    ADD   EBX, 1
    ADC   ECX, 0

    LOCK  CMPXCHG8B qword ptr [EDI]

    JNZ   @TryOutStart

    MOV   EAX, EBX
    MOV   EDX, ECX

    POP   EDI
    POP   EBX
end;
end;

//------------------------------------------------------------------------------

Function InterlockedDecrement64(I: Pointer): UInt64;
begin
asm
          PUSH  EBX
          PUSH  EDI

          MOV   EDI, EAX

    @TryOutStart:

          MOV   EAX, dword ptr [EDI]
          MOV   EDX, dword ptr [EDI + 4]

          MOV   EBX, EAX
          MOV   ECX, EDX

          SUB   EBX, 1
          SBB   ECX, 0

    LOCK  CMPXCHG8B qword ptr [EDI]

          JNZ   @TryOutStart

          MOV   EAX, EBX
          MOV   EDX, ECX

          POP   EDI
          POP   EBX
end;
end;

//------------------------------------------------------------------------------

Function InterlockedExchangeAdd64(I: Pointer; B: UInt64): UInt64;
begin
asm
          PUSH  EBX
          PUSH  EDI

          MOV   EDI, EAX

    @TryOutStart:

          MOV   EBX, dword ptr [B]
          MOV   ECX, dword ptr [B + 4]

          MOV   EAX, dword ptr [EDI]
          MOV   EDX, dword ptr [EDI + 4]

          ADD   EBX, EAX
          ADC   ECX, EDX

    LOCK  CMPXCHG8B qword ptr [EDI]

          JNZ   @TryOutStart

          POP   EDI
          POP   EBX
end;
end;

//------------------------------------------------------------------------------

Function InterlockedExchange64(I: Pointer; B: UInt64): UInt64;
begin
asm
          PUSH  EBX
          PUSH  EDI

          MOV   EDI, EAX

    @TryOutStart:

          MOV   EBX, dword ptr [B]
          MOV   ECX, dword ptr [B + 4]

          MOV   EAX, dword ptr [EDI]
          MOV   EDX, dword ptr [EDI + 4]

    LOCK  CMPXCHG8B qword ptr [EDI]

          JNZ   @TryOutStart

          POP   EDI
          POP   EBX
end;
end;

//------------------------------------------------------------------------------

Function InterlockedCompareExchange64(I: Pointer; Exchange,
                                   Comparand: UInt64): UInt64;
begin
asm
    PUSH  EBX
    PUSH  EDI

    MOV   EDI, EAX

    MOV   EAX, dword ptr [Comparand]
    MOV   EDX, dword ptr [Comparand + 4]

    MOV   EBX, dword ptr [Exchange]
    MOV   ECX, dword ptr [Exchange + 4]

    LOCK  CMPXCHG8B qword ptr [EDI]

    POP   EDI
    POP   EBX
end;
end;

{$endif}

{$endif}

{$ifdef userwlocks}

{$if defined(Windows)}
procedure InitializeSRWLock(SRWLock:PPasMPSRWLock); stdcall; external 'kernel32.dll' name 'InitializeSRWLock';
procedure AcquireSRWLockShared(SRWLock:PPasMPSRWLock); stdcall; external 'kernel32.dll' name 'AcquireSRWLockShared';
function TryAcquireSRWLockShared(SRWLock:PPasMPSRWLock):bool; stdcall; external 'kernel32.dll' name 'TryAcquireSRWLockShared';
procedure ReleaseSRWLockShared(SRWLock:PPasMPSRWLock); stdcall; external 'kernel32.dll' name 'ReleaseSRWLockShared';
procedure AcquireSRWLockExclusive(SRWLock:PPasMPSRWLock); stdcall; external 'kernel32.dll' name 'AcquireSRWLockExclusive';
function TryAcquireSRWLockExclusive(SRWLock:PPasMPSRWLock):bool; stdcall; external 'kernel32.dll' name 'TryAcquireSRWLockExclusive';
procedure ReleaseSRWLockExclusive(SRWLock:PPasMPSRWLock); stdcall; external 'kernel32.dll' name 'ReleaseSRWLockExclusive';
{$endif}

{ TNetRWVariable }

constructor TNetRWVariable.Create;
begin
  {$if defined(Windows)}
  InitializeSRWLock(@fSRWLock);
  {$elseif defined(Unix)}
  pthread_rwlock_init(@fReadWriteLock,nil);
  {$ifend}
end;

destructor TNetRWVariable.Destroy;
begin
  {$if defined(Windows)}
  {$elseif defined(Unix)}
  pthread_rwlock_destroy(@fReadWriteLock);
  {$ifend}
  inherited Destroy;
end;

procedure TNetRWVariable.BeginRead;
{$if defined(Windows)}
begin
  AcquireSRWLockShared(@fSRWLock);
end;
{$elseif defined(Unix)}
begin
  pthread_rwlock_rdlock(@fReadWriteLock);
end;
{$ifend}

procedure TNetRWVariable.EndRead;
{$if defined(Windows)}
begin
  ReleaseSRWLockShared(@fSRWLock);
end;
{$elseif defined(Unix)}
begin
  pthread_rwlock_unlock(@fReadWriteLock);
end;
{$ifend}

procedure TNetRWVariable.BeginWrite;
{$if defined(Windows)}
begin
  AcquireSRWLockExclusive(@fSRWLock);
end;
{$elseif defined(Unix)}
begin
  pthread_rwlock_wrlock(@fReadWriteLock);
end;
{$ifend}

procedure TNetRWVariable.EndWrite;
{$if defined(Windows)}
begin
  ReleaseSRWLockExclusive(@fSRWLock);
end;
{$elseif defined(Unix)}
begin
  pthread_rwlock_unlock(@fReadWriteLock);
end;
{$ifend}

{$endif}

{ TAtomicNumeric }

function TAtomicNumeric.AddValue(const aValue: T): T;
begin
  {$if defined(AtomicAsRW)}
  BeginWrite;
  try
    FValue += aValue;
    Result := FValue;
  finally
    EndWrite;
  end;
  {$elseif defined(AtomicAsInterlocked)}
  Result := Add(FValue, ST(aValue));
  {$else}
  Lock;
  try
    FValue += aValue;
    Result := FValue;
  finally
    UnLock;
  end;
  {$ifend}
end;

function TAtomicNumeric.SubValue(const aValue: T): T;
begin
  {$if defined(AtomicAsRW)}
  BeginWrite;
  try
    FValue -= aValue;
    Result := FValue;
  finally
    EndWrite;
  end;
  {$elseif defined(AtomicAsInterlocked)}
  Result := Sub(FValue, ST(aValue));
  {$else}
  Lock;
  try
    FValue -= aValue;
    Result := FValue;
  finally
    UnLock;
  end;
  {$ifend}
end;

function TAtomicNumeric.AndValue(const aValue: T): T;
begin
  {$if defined(AtomicAsRW)}
  BeginWrite;
  try
    FValue := FValue and aValue;
    Result := FValue;
  finally
    EndWrite;
  end;
  {$elseif defined(AtomicAsInterlocked)}
  {$ifdef USELIBINTERLOCKEDOPS}
  Result := InterlockedAnd(FValue, ST(aValue));
  {$else}
  Result := Exchange(FValue, CompareExchange(FValue, 0,0) and aValue);
  {$endif}
  {$else}
  Lock;
  try
    FValue := FValue and aValue;
    Result := FValue;
  finally
    UnLock;
  end;
  {$ifend}
end;

function TAtomicNumeric.OrValue(const aValue: T): T;
begin
  {$if defined(AtomicAsRW)}
  BeginWrite;
  try
    FValue := FValue or aValue;
    Result := FValue;
  finally
    EndWrite;
  end;
  {$elseif defined(AtomicAsInterlocked)}
  {$ifdef USELIBINTERLOCKEDOPS}
  Result := InterlockedOr(FValue, ST(aValue));
  {$else}
  Result := Exchange(FValue, CompareExchange(FValue, 0,0) or aValue);
  {$endif}
  {$else}
  Lock;
  try
    FValue := FValue or aValue;
    Result := FValue;
  finally
    UnLock;
  end;
  {$ifend}
end;

function TAtomicNumeric.XorValue(const aValue: T): T;
begin
  {$if defined(AtomicAsRW)}
  BeginWrite;
  try
    FValue := FValue xor aValue;
    Result := FValue;
  finally
    EndWrite;
  end;
  {$elseif defined(AtomicAsInterlocked)}
  {$ifdef USELIBINTERLOCKEDOPS}
  Result := InterlockedXor(FValue, ST(aValue));
  {$else}
  Result := Exchange(FValue, CompareExchange(FValue, 0,0) xor aValue);
  {$endif}
  {$else}
  Lock;
  try
    FValue := FValue xor aValue;
    Result := FValue;
  finally
    UnLock;
  end;
  {$ifend}
end;

function TAtomicNumeric.ShlValue(aCount: Integer): T;
begin
  {$if defined(AtomicAsRW)}
  BeginWrite;
  try
    FValue := FValue shl aCount;
    Result := FValue;
  finally
    EndWrite;
  end;
  {$elseif defined(AtomicAsInterlocked)}
  {$ifdef USELIBINTERLOCKEDOPS}
  Result := InterlockedShl(FValue, aCount);
  {$else}
  Result := Exchange(FValue, CompareExchange(FValue, 0,0) shl aCount);
  {$endif}
  {$else}
  Lock;
  try
    FValue := FValue shl aCount;
    Result := FValue;
  finally
    UnLock;
  end;
  {$ifend}
end;

function TAtomicNumeric.ShrValue(aCount: Integer): T;
begin
  {$if defined(AtomicAsRW)}
  BeginWrite;
  try
    FValue := FValue shr aCount;
    Result := FValue;
  finally
    EndWrite;
  end;
  {$elseif defined(AtomicAsInterlocked)}
  {$ifdef USELIBINTERLOCKEDOPS}
  Result := InterlockedShr(FValue, aCount);
  {$else}
  Result := Exchange(FValue, CompareExchange(FValue, 0,0) shr aCount);
  {$endif}
  {$else}
  Lock;
  try
    FValue := FValue shr aCount;
    Result := FValue;
  finally
    UnLock;
  end;
  {$ifend}
end;

function TAtomicNumeric.NotValue: T;
begin
  {$if defined(AtomicAsRW)}
  BeginWrite;
  try
    FValue := not FValue;
    Result := FValue;
  finally
    EndWrite;
  end;
  {$elseif defined(AtomicAsInterlocked)}
  {$ifdef USELIBINTERLOCKEDOPS}
  Result := InterlockedNot(FValue);
  {$else}
  Result := Exchange(FValue, not CompareExchange(FValue, 0,0));
  {$endif}
  {$else}
  Lock;
  try
    FValue := not FValue;
    Result := FValue;
  finally
    UnLock;
  end;
  {$ifend}
end;

procedure TAtomicNumeric.IncValue;
begin
  {$if defined(AtomicAsRW)}
  BeginWrite;
  try
    Inc(FValue);
  finally
    EndWrite;
  end;
  {$elseif defined(AtomicAsInterlocked)}
  Increment(FValue);
  {$else}
  Lock;
  try
    Inc(FValue);
  finally
    UnLock;
  end;
  {$ifend}
end;

procedure TAtomicNumeric.DecValue;
begin
  {$if defined(AtomicAsRW)}
  BeginWrite;
  try
    Dec(FValue);
  finally
    EndWrite;
  end;
  {$elseif defined(AtomicAsInterlocked)}
  Decrement(FValue);
  {$else}
  Lock;
  try
    Dec(FValue);
  finally
    UnLock;
  end;
  {$ifend}
end;

{ TAtomicVariable }

function TAtomicVariable.GetValue: T;
begin
  {$if defined(AtomicAsRW)}
  BeginRead;
  try
    Result := FValue;
  finally
    EndRead;
  end;
  {$elseif defined(AtomicAsInterlocked)}
  Result := T(Read(FValue));
  {$else}
  Lock;
  try
    Result := FValue;
  finally
    UnLock;
  end;
  {$ifend}
end;

procedure TAtomicVariable.SetValue(AValue: T);
begin
  {$if defined(AtomicAsRW)}
  BeginWrite;
  try
    FValue := AValue;
  finally
    EndWrite;
  end;
  {$elseif defined(AtomicAsInterlocked)}
  Write(FValue, ST(AValue));
  {$else}
  Lock;
  try
    FValue := AValue;
  finally
    UnLock;
  end;
  {$ifend}
end;

constructor TAtomicVariable.Create(aValue: T);
begin
  inherited Create;
  FValue := aValue;
end;

{ TNetInterlocked }

class function TNetInterlocked.InterlockedAnd(var A: QWord; B: QWord): QWord;
begin
   Result := QWord(InterlockedAnd64(@A, Uint64(B)));
end;

class function TNetInterlocked.InterlockedAnd(var A: Int64; B: Int64): Int64;
begin
   Result := Int64(InterlockedAnd64(@A, Uint64(B)));
end;

class function TNetInterlocked.InterlockedAnd(var A: Int32; B: Int32): Int32;
begin
   Result := Int64(InterlockedAnd32(@A, UInt32(B)));
end;

class function TNetInterlocked.InterlockedAnd(var A: Cardinal; B: Cardinal): Cardinal;
begin
   Result := Cardinal(InterlockedAnd32(@A, UInt32(B)));
end;

class function TNetInterlocked.InterlockedAnd(var A: LongBool; B: LongBool
  ): LongBool;
begin
  Result := LongBool(InterlockedAnd32(@A, UInt32(B)));
end;

class function TNetInterlocked.InterlockedOr(var A: QWord; B: QWord): QWord;
begin
  Result := QWord(InterlockedOr64(@A, Uint64(B)));
end;

class function TNetInterlocked.InterlockedOr(var A: Int64; B: Int64): Int64;
begin
  Result := Int64(InterlockedOr64(@A, Uint64(B)));
end;

class function TNetInterlocked.InterlockedOr(var A: Int32; B: Int32): Int32;
begin
  Result := Int32(InterlockedOr32(@A, Uint32(B)));
end;

class function TNetInterlocked.InterlockedOr(var A: Cardinal; B: Cardinal
  ): Cardinal;
begin
  Result := Cardinal(InterlockedOr32(@A, Uint32(B)));
end;

class function TNetInterlocked.InterlockedOr(var A: LongBool; B: LongBool
  ): LongBool;
begin
  Result := LongBool(InterlockedOr32(@A, Uint32(B)));
end;

class function TNetInterlocked.InterlockedXor(var A: QWord; B: QWord): QWord;
begin
  Result := QWord(InterlockedXor64(@A, Uint64(B)));
end;

class function TNetInterlocked.InterlockedXor(var A: Int64; B: Int64): Int64;
begin
  Result := Int64(InterlockedXor64(@A, Uint64(B)));
end;

class function TNetInterlocked.InterlockedXor(var A: Int32; B: Int32): Int32;
begin
  Result := Int32(InterlockedXor32(@A, Uint32(B)));
end;

class function TNetInterlocked.InterlockedXor(var A: Cardinal; B: Cardinal
  ): Cardinal;
begin
  Result := Cardinal(InterlockedXor32(@A, Uint32(B)));
end;

class function TNetInterlocked.InterlockedXor(var A: LongBool; B: LongBool
  ): LongBool;
begin
  Result := LongBool(InterlockedXor32(@A, Uint32(B)));
end;

class function TNetInterlocked.InterlockedNot(var A: QWord): QWord;
begin
  Result := QWord(InterlockedNot64(@A));
end;

class function TNetInterlocked.InterlockedNot(var A: Int64): Int64;
begin
  Result := Int64(InterlockedNot64(@A));
end;

class function TNetInterlocked.InterlockedNot(var A: Int32): Int32;
begin
  Result := Int32(InterlockedNot32(@A));
end;

class function TNetInterlocked.InterlockedNot(var A: Cardinal): Cardinal;
begin
  Result := Cardinal(InterlockedNot32(@A));
end;

class function TNetInterlocked.InterlockedNot(var A: LongBool): LongBool;
begin
  Result := LongBool(InterlockedNot32(@A));
end;

class function TNetInterlocked.InterlockedShr(var A: QWord; Cnt: Integer
  ): QWord;
begin
  Result := QWord(InterlockedShr64(@A, Cnt));
end;

class function TNetInterlocked.InterlockedShr(var A: Int64; Cnt: Integer
  ): Int64;
begin
  Result := Int64(InterlockedShr64(@A, Cnt));
end;

class function TNetInterlocked.InterlockedShr(var A: Int32; Cnt: Integer
  ): Int32;
begin
  Result := Int32(InterlockedShr32(@A, Cnt));
end;

class function TNetInterlocked.InterlockedShr(var A: Cardinal; Cnt: Integer
  ): Cardinal;
begin
  Result := Cardinal(InterlockedShr32(@A, Cnt));
end;

class function TNetInterlocked.InterlockedShr(var A: LongBool; {%H-}Cnt: Integer
  ): LongBool;
begin
  Result := InterlockedStoreBool(@A, False);
end;

class function TNetInterlocked.InterlockedShl(var A: QWord; Cnt: Integer
  ): QWord;
begin
  Result := QWord(InterlockedShl64(@A, Cnt));
end;

class function TNetInterlocked.InterlockedShl(var A: Int64; Cnt: Integer
  ): Int64;
begin
  Result := Int64(InterlockedShl64(@A, Cnt));
end;

class function TNetInterlocked.InterlockedShl(var A: Int32; Cnt: Integer
  ): Int32;
begin
  Result := Int32(InterlockedShl32(@A, Cnt));
end;

class function TNetInterlocked.InterlockedShl(var A: Cardinal; Cnt: Integer
  ): Cardinal;
begin
  Result := Cardinal(InterlockedShl32(@A, Cnt));
end;

class function TNetInterlocked.InterlockedShl(var A: LongBool; {%H-}Cnt: Integer
  ): LongBool;
begin
  Result := InterlockedStoreBool(@A, False);
end;

class function TNetInterlocked.Increment(var Destination: Int32): Int32;
begin
  result:=InterlockedIncrement(Destination);
end;

class function TNetInterlocked.Increment(var Destination: UInt32): UInt32;
begin
  result:=InterlockedIncrement(Destination);
end;

class function TNetInterlocked.Increment(var Destination: Int64): Int64;
begin
  {$ifdef x64}
  result:=InterlockedIncrement64(Destination);
  {$else}
  {$ifdef USELIBINTERLOCKEDOPS}
  result:=InterlockedIncrement64(@Destination);
  {$else}
  {$MESSAGE FATAL 'Unsupported processor.'}
  {$endif}
  {$endif}
end;

class function TNetInterlocked.Increment(var Destination: UInt64): UInt64;
begin
  {$ifdef x64}
  result:=InterlockedIncrement64(Destination);
  {$else}
  {$ifdef USELIBINTERLOCKEDOPS}
  result:=InterlockedIncrement64(@Destination);
  {$else}
  {$MESSAGE FATAL 'Unsupported processor.'}
  {$endif}
  {$endif}
end;

class function TNetInterlocked.Decrement(var Destination: Int32): Int32;
begin
  result:=InterlockedDecrement(Destination);
end;

class function TNetInterlocked.Decrement(var Destination: UInt32): UInt32;
begin
  result:=InterlockedDecrement(Destination);
end;

class function TNetInterlocked.Decrement(var Destination: Int64): Int64;
begin
  {$ifdef x64}
  result:=InterlockedDecrement64(Destination);
  {$else}
  {$ifdef USELIBINTERLOCKEDOPS}
  result:=InterlockedDecrement64(@Destination);
  {$else}
  {$MESSAGE FATAL 'Unsupported processor.'}
  {$endif}
  {$endif}
end;

class function TNetInterlocked.Decrement(var Destination: UInt64): UInt64;
begin
  {$ifdef x64}
  result:=InterlockedDecrement64(Destination);
  {$else}
  {$ifdef USELIBINTERLOCKEDOPS}
  result:=InterlockedDecrement64(@Destination);
  {$else}
  {$MESSAGE FATAL 'Unsupported processor.'}
  {$endif}
  {$endif}
end;

class function TNetInterlocked.Add(var Destination: Int32;
  const Value: Int32): Int32;
begin
  result:=InterlockedExchangeAdd(Destination,Value);
end;

class function TNetInterlocked.Add(var Destination: UInt32;
  const Value: UInt32): UInt32;
begin
  result:=InterlockedExchangeAdd(Destination,Value);
end;

class function TNetInterlocked.Add(var Destination: Int64; const Value: Int64
  ): Int64;
begin
  {$ifdef x64}
  result:=InterlockedExchangeAdd64(Destination,Value);
  {$else}
  {$ifdef USELIBINTERLOCKEDOPS}
  result:=InterlockedExchangeAdd64(@Destination, Value);
  {$else}
  {$MESSAGE FATAL 'Unsupported processor.'}
  {$endif}
  {$endif}
end;

class function TNetInterlocked.Add(var Destination: UInt64; const Value: UInt64
  ): UInt64;
begin
  {$ifdef x64}
  result:=InterlockedExchangeAdd64(Destination,Value);
  {$else}
  {$ifdef USELIBINTERLOCKEDOPS}
  result:=InterlockedExchangeAdd64(@Destination, Value);
  {$else}
  {$MESSAGE FATAL 'Unsupported processor.'}
  {$endif}
  {$endif}
end;

class function TNetInterlocked.Sub(var Destination: Int32;
  const Value: Int32): Int32;
begin
  result:=InterlockedExchangeAdd(Destination,-Value);
end;

class function TNetInterlocked.Sub(var Destination: UInt32;
  const Value: UInt32): UInt32;
begin
  result:=UInt32(Int32(InterlockedExchangeAdd(Destination,-Int32(Value))));
end;

class function TNetInterlocked.Sub(var Destination: Int64; const Value: Int64
  ): Int64;
begin
  {$ifdef x64}
  result:=InterlockedExchangeAdd64(Destination,-Value);
  {$else}
  {$ifdef USELIBINTERLOCKEDOPS}
  result:=InterlockedExchangeAdd64(@Destination,-Value);
  {$else}
  {$MESSAGE FATAL 'Unsupported processor.'}
  {$endif}
  {$endif}
end;

class function TNetInterlocked.Sub(var Destination: UInt64; const Value: UInt64
  ): UInt64;
begin
  {$ifdef x64}
  result:=UInt64(Int64(InterlockedExchangeAdd64(Destination,-Int64(Value))));
  {$else}
  {$ifdef USELIBINTERLOCKEDOPS}
  result:=UInt64(Int64(InterlockedExchangeAdd64(@Destination,-Int64(Value))));
  {$else}
  {$MESSAGE FATAL 'Unsupported processor.'}
  {$endif}
  {$endif}
end;

class function TNetInterlocked.Exchange(var Destination: Int32;
  const Source: Int32): Int32;
begin
  result:=InterlockedExchange(Destination,Source);
end;

class function TNetInterlocked.Exchange(var Destination: UInt32;
  const Source: UInt32): UInt32;
begin
  result:=InterlockedExchange(Destination,Source);
end;

class function TNetInterlocked.Exchange(var Destination: Int64;
  const Source: Int64): Int64;
begin
  {$ifdef x64}
  result:=InterlockedExchange64(Destination,Source);
  {$else}
  {$ifdef USELIBINTERLOCKEDOPS}
  result:=InterlockedExchange64(@Destination,Source);
  {$else}
  {$MESSAGE FATAL 'Unsupported processor.'}
  {$endif}
  {$endif}
end;

class function TNetInterlocked.Exchange(var Destination: UInt64;
  const Source: UInt64): UInt64;
begin
  {$ifdef x64}
  result:=InterlockedExchange64(Destination,Source);
  {$else}
  {$ifdef USELIBINTERLOCKEDOPS}
  result:=InterlockedExchange64(@Destination,Source);
  {$else}
  {$MESSAGE FATAL 'Unsupported processor.'}
  {$endif}
  {$endif}
end;

class function TNetInterlocked.Exchange(var Destination: pointer;
  const Source: pointer): pointer;
begin
  result:=InterlockedExchange(Destination,Source);
end;

class function TNetInterlocked.Exchange(var Destination: TObject;
  const Source: TObject): TObject;
begin
  result:=TObject(InterlockedExchange(Pointer(Destination),Pointer(Source)));
end;

class function TNetInterlocked.Exchange(var Destination: LongBool;
  const Source: LongBool): LongBool;
begin
  result:=LongBool(Int32(InterlockedExchange(Int32(Destination),Int32(Source))));
end;

class function TNetInterlocked.CompareExchange(var Destination: Int32;
  const NewValue, Comperand: Int32): Int32;
begin
  result:=InterlockedCompareExchange(Destination,NewValue,Comperand);
end;

class function TNetInterlocked.CompareExchange(var Destination: UInt32;
  const NewValue, Comperand: UInt32): UInt32;
begin
  result:=InterlockedCompareExchange(Destination,NewValue,Comperand);
end;

class function TNetInterlocked.CompareExchange(var Destination: Int64;
  const NewValue, Comperand: Int64): Int64;
begin
  {$ifdef x64}
  result:=InterlockedCompareExchange64(Destination,NewValue,Comperand);
  {$else}
  {$ifdef USELIBINTERLOCKEDOPS}
  result:=InterlockedCompareExchange64(@Destination,NewValue,Comperand);
  {$else}
  {$MESSAGE FATAL 'Unsupported processor.'}
  {$endif}
  {$endif}
end;

class function TNetInterlocked.CompareExchange(var Destination: UInt64;
  const NewValue, Comperand: UInt64): UInt64;
begin
  {$ifdef x64}
  result:=InterlockedCompareExchange64(Destination,NewValue,Comperand);
  {$else}
  {$ifdef USELIBINTERLOCKEDOPS}
  result:=InterlockedCompareExchange64(@Destination,NewValue,Comperand);
  {$else}
  {$MESSAGE FATAL 'Unsupported processor.'}
  {$endif}
  {$endif}
end;

class function TNetInterlocked.CompareExchange(var Destination: pointer;
  const NewValue, Comperand: pointer): pointer;
begin
  result:=InterlockedCompareExchangePointer(Destination,NewValue,Comperand);
end;

class function TNetInterlocked.CompareExchange(var Destination: TObject;
  const NewValue, Comperand: TObject): TObject;
begin
  result:=TObject(InterlockedCompareExchangePointer(pointer(Destination),
                                                    pointer(NewValue),
                                                    pointer(Comperand)));
end;

class function TNetInterlocked.CompareExchange(var Destination: LongBool;
  const NewValue, Comperand: LongBool): LongBool;
begin
  result:=LongBool(Int32(InterlockedCompareExchange(Int32(Destination),
                                                    Int32(NewValue),
                                                    Int32(Comperand))));
end;

class function TNetInterlocked.Read(var Source: Int32): Int32;
begin
{$ifdef USELIBINTERLOCKEDOPS}
  result := Int32(InterlockedLoad32(@Source));
{$else}
  result:=InterlockedCompareExchange(Source,0,0);
{$endif}
end;

class function TNetInterlocked.Read(var Source: UInt32): UInt32;
begin
{$ifdef USELIBINTERLOCKEDOPS}
result := InterlockedLoad32(@Source);
{$else}
 result:=UInt32(InterlockedCompareExchange(Int32(Source),0,0));
{$endif}
end;

class function TNetInterlocked.Read(var Source: Int64): Int64;
begin
{$ifdef USELIBINTERLOCKEDOPS}
  result := Int64(InterlockedLoad64(@Source));
{$else}
  result:=InterlockedCompareExchange64(Source,0,0);
{$endif}
end;

class function TNetInterlocked.Read(var Source: UInt64): UInt64;
begin
{$ifdef USELIBINTERLOCKEDOPS}
  result := UInt64(InterlockedLoad64(@Source));
{$else}
  result:=UInt64(InterlockedCompareExchange64(Int64(Source),0,0));
{$endif}
end;

class function TNetInterlocked.Read(var Source: pointer): pointer;
begin
  result:=InterlockedCompareExchangePointer(Pointer(Source),
                                            Pointer(PtrInt(0)),
                                            Pointer(PtrInt(0)));
end;

class function TNetInterlocked.Read(var Source: TObject): TObject;
begin
  result:=TObject(InterlockedCompareExchangePointer(Pointer(Source),
                                                    Pointer(PtrInt(0)),
                                                    Pointer(PtrInt(0))));
end;

class function TNetInterlocked.Read(var Source: LongBool): LongBool;
begin
{$ifdef USELIBINTERLOCKEDOPS}
  result := LongBool(InterlockedLoad32(@Source));
{$else}
  result:=LongBool(InterlockedCompareExchange(Int32(Source),0,0));
{$endif}
end;

class function TNetInterlocked.Write(var Destination: Int32;
  const Source: Int32): Int32;
begin
{$ifdef USELIBINTERLOCKEDOPS}
  result := Int32(InterlockedStore32(@Destination, Uint32(Source)));
{$else}
  result := Exchange(Destination, Source);
{$endif}
end;

class function TNetInterlocked.Write(var Destination: UInt32;
  const Source: UInt32): UInt32;
begin
{$ifdef USELIBINTERLOCKEDOPS}
  result := InterlockedStore32(@Destination, Source);
{$else}
  result := Exchange(Destination, Source);
{$endif}
end;

class function TNetInterlocked.Write(var Destination: Int64;
  const Source: Int64): Int64;
begin
{$ifdef USELIBINTERLOCKEDOPS}
  result := Int64(InterlockedStore64(@Destination, UInt64(Source)));
{$else}
  result := Exchange(Destination, Source);
{$endif}
end;

class function TNetInterlocked.Write(var Destination: UInt64;
  const Source: UInt64): UInt64;
begin
{$ifdef USELIBINTERLOCKEDOPS}
  result := InterlockedStore64(@Destination, Source);
{$else}
  result := Exchange(Destination, Source);
{$endif}
end;

class function TNetInterlocked.Write(var Destination: pointer;
  const Source: pointer): pointer;
begin
  result := Exchange(Destination, Source);
end;

class function TNetInterlocked.Write(var Destination: TObject;
  const Source: TObject): TObject;
begin
  result := Exchange(Destination, Source);
end;

class function TNetInterlocked.Write(var Destination: LongBool;
  const Source: LongBool): LongBool;
begin
{$ifdef USELIBINTERLOCKEDOPS}
  result := LongBool(InterlockedStore32(@Destination, Uint32(Source)));
{$else}
  result := Exchange(Destination, Source);
{$endif}
end;

{ TThreadActiveFastBaseCollection }

procedure TThreadActiveFastBaseCollection.DoChange;
begin
  if FUpdateCnt = 0 then
   if assigned(FOnChange) then
     FOnChange(Self);
end;

function TThreadActiveFastBaseCollection.GetOnChange: TNotifyEvent;
begin
  Lock;
  try
    Result := FOnChange;
  finally
    UnLock;
  end;
end;

procedure TThreadActiveFastBaseCollection.SetOnChange(AValue: TNotifyEvent);
begin
  Lock;
  try
    FOnChange := AValue;
  finally
    UnLock;
  end;
end;

procedure TThreadActiveFastBaseCollection.SetCount(AValue: integer);
begin
  inherited SetCount(AValue);
  DoChange;
end;

constructor TThreadActiveFastBaseCollection.Create;
begin
  inherited Create;
  FOnChange:= nil;
  FUpdateCnt:= 0;
end;

procedure TThreadActiveFastBaseCollection.BeginUpdate;
begin
  Lock;
  Inc(FUpdateCnt);
end;

procedure TThreadActiveFastBaseCollection.EndUpdate;
begin
  Dec(FUpdateCnt);
  UnLock;
  DoChange;
end;

function TThreadActiveFastBaseCollection.Add(const Obj: T): Integer;
begin
  Result:=inherited Add(Obj);
  DoChange;
end;

function TThreadActiveFastBaseCollection.Remove(const obj: T): integer;
begin
  Result:=inherited Remove(obj);
  DoChange;
end;

procedure TThreadActiveFastBaseCollection.Delete(Ind: integer);
begin
  inherited Delete(Ind);
  DoChange;
end;

procedure TThreadActiveFastBaseCollection.Clear;
begin
  inherited Clear;
  DoChange;
end;

procedure TThreadActiveFastBaseCollection.Extract(Ind: integer);
begin
  inherited Extract(Ind);
  DoChange;
end;

procedure TThreadActiveFastBaseCollection.Pack;
begin
  inherited Pack;
  DoChange;
end;

{ TThreadActiveFastBaseSeq }

procedure TThreadActiveFastBaseSeq.DoChange;
begin
  if FUpdateCnt = 0 then
   if assigned(FOnChange) then
     FOnChange(Self);
end;

function TThreadActiveFastBaseSeq.GetOnChange: TNotifyEvent;
begin
  Lock;
  try
    Result := FOnChange;
  finally
    UnLock;
  end;
end;

procedure TThreadActiveFastBaseSeq.SetOnChange(AValue: TNotifyEvent);
begin
  Lock;
  try
    FOnChange := AValue;
  finally
    UnLock;
  end;
end;

constructor TThreadActiveFastBaseSeq.Create;
begin
  inherited Create;
  FOnChange:= nil;
  FUpdateCnt:= 0;
end;

procedure TThreadActiveFastBaseSeq.BeginUpdate;
begin
  Lock;
  Inc(FUpdateCnt);
end;

procedure TThreadActiveFastBaseSeq.EndUpdate;
begin
  Dec(FUpdateCnt);
  UnLock;
  DoChange;
end;

procedure TThreadActiveFastBaseSeq.Push_back(const O: TObject);
begin
  inherited Push_back(O);
  DoChange;
end;

procedure TThreadActiveFastBaseSeq.Push_front(const O: TObject);
begin
  inherited Push_front(O);
  DoChange;
end;

function TThreadActiveFastBaseSeq.InsertBefore(loc: TIteratorObject; o: TObject
  ): TIteratorObject;
begin
  Result:=inherited InsertBefore(loc, o);
  DoChange;
end;

function TThreadActiveFastBaseSeq.InsertAfter(loc: TIteratorObject; o: TObject
  ): TIteratorObject;
begin
  Result:=inherited InsertAfter(loc, o);
  DoChange;
end;

{ TNetAutoReferenceList }

function TNetAutoReferenceList.GetCount : Integer;
begin
  Result := FRefCnt.Value;
end;

procedure TNetAutoReferenceList.UnRegister(v : TNetReferencedObject);
begin
  FRefCnt.DecValue;
end;

constructor TNetAutoReferenceList.Create;
begin
  FRefCnt := TAtomicInteger.Create(0);
end;

destructor TNetAutoReferenceList.Destroy;
begin
  FRefCnt.Free;
  inherited Destroy;
end;

procedure TNetAutoReferenceList.Add(const O : TNetAutoReferencedObject);
begin
  FRefCnt.IncValue;
  O.OnRemove := @(Self.UnRegister);
end;

procedure TNetAutoReferenceList.CleanDead;
begin
  // Do nothing
end;

{ TThreadActiveNumeric }

procedure TThreadActiveNumeric.DoOnChange;
begin
  if Assigned(FOnChange) then
    FOnChange(Self);
end;

procedure TThreadActiveNumeric.SetValue(const AValue: T);
begin
  inherited SetValue(AValue);
  DoOnChange;
end;

procedure TThreadActiveNumeric.IncValue;
begin
  inherited IncValue;
  DoOnChange;
end;

procedure TThreadActiveNumeric.DecValue;
begin
  inherited DecValue;
  DoOnChange;
end;

procedure TThreadActiveNumeric.IncValue(IncSz: T);
begin
  inherited IncValue(IncSz);
  DoOnChange;
end;

procedure TThreadActiveNumeric.DecValue(DecSz: T);
begin
  inherited DecValue(DecSz);
  DoOnChange;
end;

procedure TThreadActiveNumeric.OrValue(mask: T);
begin
  inherited OrValue(mask);
  DoOnChange;
end;

procedure TThreadActiveNumeric.AndValue(mask: T);
begin
  inherited AndValue(mask);
  DoOnChange;
end;

procedure TThreadActiveNumeric.XorValue(mask: T);
begin
  inherited XorValue(mask);
  DoOnChange;
end;

procedure TThreadActiveNumeric.NotValue;
begin
  inherited NotValue;
  DoOnChange;
end;

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

procedure TThreadNumeric.OrValue(mask: T);
begin
  Lock;
  try
    FValue := FValue or mask;
  finally
    UnLock;
  end;
end;

procedure TThreadNumeric.AndValue(mask: T);
begin
  Lock;
  try
    FValue := FValue and mask;
  finally
    UnLock;
  end;
end;

procedure TThreadNumeric.XorValue(mask: T);
begin
  Lock;
  try
    FValue := FValue xor mask;
  finally
    UnLock;
  end;
end;

procedure TThreadNumeric.NotValue();
begin
  Lock;
  try
    FValue := not  FValue;
  finally
    UnLock;
  end;
end;

function TThreadNumeric.BitwiseOr(mask: T): T;
begin
  Lock;
  try
    Result := FValue or mask;
  finally
    UnLock;
  end;
end;

function TThreadNumeric.BitwiseAnd(mask: T): T;
begin
  Lock;
  try
    Result := FValue and mask;
  finally
    UnLock;
  end;
end;

function TThreadNumeric.BitwiseXor(mask: T): T;
begin
  Lock;
  try
    Result := FValue xor mask;
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
  Result := TNetReferencedObject(obj).NoReferences;
end;

{ TNetReferencedObject }

constructor TNetReferencedObject.Create;
begin
  inherited Create;
  mReferenceCount:= TAtomicInteger.Create(1);
end;

destructor TNetReferencedObject.Destroy;
begin
  mReferenceCount.Free;
  inherited Destroy;
end;

procedure TNetReferencedObject.IncReference;
begin
  mReferenceCount.IncValue;
end;

procedure TNetReferencedObject.DecReference;
begin
  mReferenceCount.DecValue;
end;

function TNetReferencedObject.HasReferences: Boolean;
begin
  Result := mReferenceCount.Value > 0;
end;

function TNetReferencedObject.HasExternReferences: Boolean;
begin
  Result := mReferenceCount.Value > 1;
end;

function TNetReferencedObject.NoReferences: Boolean;
begin
  Result := mReferenceCount.Value <= 0;
end;

{ TNetAutoReferencedObject }

procedure TNetAutoReferencedObject.DecReference;
begin
  inherited DecReference;

  if not HasReferences then
  begin
    if Assigned(FOnRemove) then
       FOnRemove(Self);
    Self.Free;
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

procedure TThreadStringList.SaveToFile(const FN: String);
begin
  Lock;
  try
    FStringList.SaveToFile(FN);
  finally
    UnLock;
  end;
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

procedure TThreadStringList.ConcatText(const aText: String);
begin
  Lock;
  try
    FStringList.Text := FStringList.Text + aText;
  finally
    UnLock;
  end;
  DoChange;
end;

end.


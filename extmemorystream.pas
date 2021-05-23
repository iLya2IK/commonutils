{ ExtMemoryStream
   stream class with owned memory buffer
   Copyright (c) 2021 Ilya Medvedkov   }

unit extmemorystream;

{$mode objfpc}{$H+}

interface

uses
  Classes, SysUtils;

type

{ TExtMemoryStream }

TExtMemoryStream = class(TCustomMemoryStream)
private
  FCapacity: PtrInt;
  FBufferInc, FBufferMin, FBufferMaxInc : Byte;
  procedure Init(ASize : PtrInt; AMinBufferSize, ABufferInitInc,
    ABufferMaxInc : Byte);
  procedure SetCapacity(NewSize : PtrInt);
public
  constructor Create; overload;
  constructor Create(ASize : PtrInt); overload;
  constructor Create(ASize : PtrInt; AMinBufferSize, ABufferInitInc,
    ABufferMaxInc : Byte); overload;
  destructor Destroy; override;

  procedure SetPtr(Ptr: Pointer; ASize: PtrInt);
  procedure SetSize(const aNewSize : Int64); override;
  procedure CopyMemory(const Src; Count : PtrInt);
  procedure Clear;
  function Write(const Buffer; Count: Longint): Longint; override;
  function Read(var Buffer; Count: LongInt): LongInt; override;
end;

implementation

const
  EXT_DEF_MIN_BUFFER_SIZE = Byte(9);
  EXT_DEF_BUFFER_INC      = Byte(10);
  EXT_DEF_BUFFER_MAX_INC  = Byte(15);

procedure TExtMemoryStream.Init(ASize : PtrInt; AMinBufferSize,
  ABufferInitInc, ABufferMaxInc: Byte);
begin
  FBufferMin := AMinBufferSize;
  FBufferInc := ABufferInitInc;
  FBufferMaxInc := ABufferMaxInc;
  FCapacity := ASize;
  if ASize > 0 then
   SetPointer(GetMem(ASize), ASize) else
   SetPointer(nil, 0);
end;

procedure TExtMemoryStream.SetCapacity(NewSize: PtrInt);
var fmem : Pointer;
begin
  if FCapacity < NewSize then
  begin
    if NewSize < PtrInt(1 shl FBufferMin) then
    begin
      FCapacity := PtrInt(1 shl FBufferMin);
    end else
    begin
      FCapacity := ((NewSize shr FBufferInc) + 1) shl FBufferInc;
      Inc(FBufferInc);
      if FBufferInc > FBufferMaxInc then FBufferInc := FBufferMaxInc;
    end;
    fmem := Memory;
    SetPointer(ReAllocMem(fmem, FCapacity), NewSize);
  end;
end;

constructor TExtMemoryStream.Create;
begin
  inherited Create;
  Init(0, EXT_DEF_MIN_BUFFER_SIZE, EXT_DEF_BUFFER_INC,
          EXT_DEF_BUFFER_MAX_INC);
end;

constructor TExtMemoryStream.Create(ASize: PtrInt);
begin
  inherited Create;
  Init(ASize, EXT_DEF_MIN_BUFFER_SIZE, EXT_DEF_BUFFER_INC,
              EXT_DEF_BUFFER_MAX_INC);
end;

constructor TExtMemoryStream.Create(ASize : PtrInt; AMinBufferSize,
  ABufferInitInc, ABufferMaxInc: Byte);
begin
  inherited Create;
  Init(ASize, AMinBufferSize, ABufferInitInc, ABufferMaxInc);
end;

destructor TExtMemoryStream.Destroy;
begin
  Clear;
  inherited Destroy;
end;

procedure TExtMemoryStream.SetPtr(Ptr: Pointer; ASize: PtrInt);
begin
  if Memory <> Ptr then
  begin
    if Assigned(Ptr) then
    begin
      if Assigned(Memory) then Freemem(Memory);
      SetPointer(Ptr, ASize);
      FCapacity:=ASize;
      Seek(0, soFromBeginning);
    end else
      Clear;
  end else
   if Assigned(Ptr) then SetSize(ASize);
end;

procedure TExtMemoryStream.SetSize(const aNewSize: Int64);
begin
  SetCapacity(aNewSize);
  SetPointer(Memory, aNewSize);
end;

procedure TExtMemoryStream.CopyMemory(const Src; Count : PtrInt);
begin
  SetSize(Count);
  Move(Src, Memory^, Count);
  Seek(0, soFromBeginning);
end;

procedure TExtMemoryStream.Clear;
begin
  if Assigned(Memory) then FreeMem(Memory);
  SetPointer(nil, 0);
  FCapacity := 0;
  Seek(0, soFromBeginning);
end;

function TExtMemoryStream.Write(const Buffer; Count: Longint): Longint;
var newSize : PtrInt;
begin
  Result:=Count;
  If (Count>0) then
  begin
    newSize := Position + Count;
    if newSize > Size then
      SetSize(newSize);
    Move(Buffer, Pointer(Memory+Position)^, Result);
    Seek(Result, soFromCurrent);
  end;
end;

function TExtMemoryStream.Read(var Buffer; Count: LongInt): LongInt;
begin
 Result:=0;
 If (Size>0) and (Position<Size) and (Position>=0) then
 begin
   Result:=Count;
   If (Result>(Size-Position)) then
     Result:=(Size-Position);
   Move(Pointer(Memory+Position)^, Buffer, Result);
   Seek(Result, soFromCurrent);
 end;
end;

end.


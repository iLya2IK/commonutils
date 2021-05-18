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

TExtMemoryStream = class(TStream)
private
  FMemory: Pointer;
  FSize, FPosition, FCapacity: PtrInt;
  FBufferInc, FBufferMin, FBufferMaxInc : DWord;
  procedure Init(ASize : PtrInt; AMinBufferSize, ABufferInitInc,
    ABufferMaxInc : DWord);
  procedure SetCapacity(NewSize : PtrInt);
protected
  Function GetSize : Int64; Override;
  function GetPosition: Int64; Override;
public
  constructor Create; overload;
  constructor Create(ASize : PtrInt); overload;
  constructor Create(ASize : PtrInt; AMinBufferSize, ABufferInitInc,
    ABufferMaxInc : DWord); overload;
  destructor Destroy; override;

  procedure SetPointer(Ptr: Pointer; ASize: PtrInt);
  procedure SetSize(const aNewSize : Int64); override;
  procedure CopyMemory(const Src; Count : PtrInt);
  procedure Clear;
  function Write(const Buffer; Count: Longint): Longint; override;
  function Read(var Buffer; Count: LongInt): LongInt; override;
  function Seek(const Offset: Int64; Origin: TSeekOrigin): Int64; override;
  property Memory: Pointer read FMemory;
end;

implementation

const
  EXT_DEF_MIN_BUFFER_SIZE = DWord(512);
  EXT_DEF_BUFFER_INC      = DWord(1024);
  EXT_DEF_BUFFER_MAX_INC  = DWord(32768);

procedure TExtMemoryStream.Init(ASize : PtrInt; AMinBufferSize,
  ABufferInitInc, ABufferMaxInc: DWord);
begin
  FSize := ASize;
  FBufferMin := AMinBufferSize;
  FBufferInc := ABufferInitInc;
  FBufferMaxInc := ABufferMaxInc;
  FCapacity := FSize;
  if FSize > 0 then
   FMemory := GetMem(FSize) else
   FMemory := nil;
end;

procedure TExtMemoryStream.SetCapacity(NewSize: PtrInt);
begin
  if FCapacity < NewSize then
  begin
    if NewSize < PtrInt(FBufferMin) then
    begin
      FCapacity := FBufferMin;
    end else
    begin
      FCapacity := (NewSize div FBufferInc + 1) * FBufferInc;
      FBufferInc := FBufferInc shl 1;
      if FBufferInc > FBufferMaxInc then FBufferInc := FBufferMaxInc;
    end;
    FMemory := ReAllocMem(FMemory, FCapacity);
  end;
end;

function TExtMemoryStream.GetSize: Int64;
begin
  Result:=FSize;
end;

function TExtMemoryStream.GetPosition: Int64;
begin
  Result:=FPosition;
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
  ABufferInitInc, ABufferMaxInc: DWord);
begin
  inherited Create;
  Init(ASize, AMinBufferSize, ABufferInitInc, ABufferMaxInc);
end;

destructor TExtMemoryStream.Destroy;
begin
  Clear;
  inherited Destroy;
end;

procedure TExtMemoryStream.SetPointer(Ptr: Pointer; ASize: PtrInt);
begin
  if FMemory <> Ptr then
  begin
    if Assigned(Ptr) then
    begin
      if Assigned(FMemory) then Freemem(FMemory);
      FMemory:=Ptr;
      FSize:=ASize;
      FCapacity:=ASize;
      FPosition:=0;
    end else
      Clear;
  end else
   if Assigned(Ptr) then SetSize(ASize);
end;

procedure TExtMemoryStream.SetSize(const aNewSize: Int64);
begin
  SetCapacity(aNewSize);
  FSize := aNewSize;
end;

procedure TExtMemoryStream.CopyMemory(const Src; Count : PtrInt);
begin
  SetSize(Count);
  Move(Src, FMemory^, Count);
  FPosition := 0;
end;

procedure TExtMemoryStream.Clear;
begin
  FSize := 0;
  FCapacity := 0;
  FPosition := 0;
  if Assigned(FMemory) then FreeMemAndNil(FMemory);
end;

function TExtMemoryStream.Write(const Buffer; Count: Longint): Longint;
var newSize : PtrInt;
begin
  Result:=Count;
  If (Count>0) then
  begin
    newSize := FPosition + Count;
    if newSize > FSize then
      SetSize(newSize);
    Move(Buffer, Pointer(FMemory+FPosition)^, Result);
    Inc(FPosition, Result);
  end;
end;

function TExtMemoryStream.Read(var Buffer; Count: LongInt): LongInt;
begin
 Result:=0;
 If (FSize>0) and (FPosition<Fsize) and (FPosition>=0) then
   begin
     Result:=Count;
     If (Result>(FSize-FPosition)) then
       Result:=(FSize-FPosition);
     Move ((FMemory+FPosition)^,Buffer,Result);
     Inc(FPosition, Result);
   end;
end;

function TExtMemoryStream.Seek(const Offset: Int64; Origin: TSeekOrigin): Int64;
begin
 Case Word(Origin) of
   soFromBeginning : FPosition:=Offset;
   soFromEnd       : FPosition:=FSize+Offset;
   soFromCurrent   : FPosition:=FPosition+Offset;
 end;
 Result:=FPosition;
end;

end.


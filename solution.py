import collections
import hashlib

__all__ = ["rollingchecksum", "weak_checksum", "patchstream", "delta",
    "blockchecksums"]


def delta(datastream, rem_signatures, blocksize=4096):
  
    rem_weak, rem_strong = rem_signatures

    deltaqueue = collections.deque()
    match = True
    matchblock = -1


    while True:
        if match and datastream is not None:
            # Всякий раз, когда есть совпадение или цикл запускается впервые,
            # заполняем часть, используя слабую чексумму вместо того,
            # чтобы прокручивать каждый отдельный байт.
            portion = collections.deque(bytes(datastream.read(blocksize)))
            checksum, a, b = weak_checksum(portion)

        try:
            # Если в файле есть две идентичные слабые контрольные суммы,
            # и соответствующий сильный хэш не встречается при первом совпадении,
            # он будет пропущен и данные будут отправлены,  но эта проблема возникает очень редко.
            matchblock = rem_weak.index(checksum, matchblock + 1)
            stronghash = hashlib.md5(bytes(portion)).hexdigest()
            matchblock = rem_strong.index(stronghash, matchblock)

            match = True
            deltaqueue.append(matchblock)

            if datastream.closed:
                break
            continue

        except ValueError:
            #Если у чексуммы не нашлось совпадений
            match = False
            try:
                if datastream:
                    # Получить следующий байт и прикрепить к части
                    newbyte = ord(datastream.read(1))
                    portion.append(newbyte)
            except TypeError:
                # Больше нет данных из файла; часть будет медленно сжиматься. 
                # С этого момента newbyte должен быть равен нулю, 
                # чтобы контрольная сумма оставалась правильной.
                newbyte = 0
                tailsize = datastream.tell() % blocksize
                datastream = None

            if datastream is None and len(portion) <= tailsize:
                #Вероятность того, что какие-либо блоки будут совпадать после этого,
                # почти равна нулю, поэтому вызовем его.
                deltaqueue.append(portion)
                break

            # Убрать лишний байт и вычислить контрольную сумму новой порции
            oldbyte = portion.popleft()
            checksum, a, b = rollingchecksum(oldbyte, newbyte, a, b, blocksize)

            # Добавим старый байт в дельту файла. Это данные, которые не были найдены
            # внутри совпадающего блока, поэтому его нужно отправить в "цель".
            try:
                deltaqueue[-1].append(oldbyte)
            except (AttributeError, IndexError):
                deltaqueue.append([oldbyte])

   # Возвращаем дельту, которая начинается с размера блока и преобразует все итерации
    # в байты.
    deltastructure = [blocksize]
    for element in deltaqueue:
        if isinstance(element, int):
            deltastructure.append(element)
        elif element:
            deltastructure.append(bytes(element))

    return deltastructure


def blockchecksums(in_stream, blocksize=4096):
    """ 
    Возвращаем список слабых и сильных хэшей для каждого блока
    определенный размер для данного потока данных.
    """
    weakhashes = list()
    stronghashes = list()
    read = in_stream.read(blocksize)

    while read:
        weakhashes.append(weak_checksum(bytes(read))[0])
        stronghashes.append(hashlib.md5(read).hexdigest())
        read = in_stream.read(blocksize)

    return weakhashes, stronghashes


def patchstream(in_stream, out_stream, delta):
    """
    Исправляем in_stream, используя предоставленную дельту, и записываем результирующий
    данные в out_stream.
    """
    blocksize = delta[0]

    for element in delta[1:]:
        if isinstance(element, int) and blocksize:
            in_stream.seek(element * blocksize)
            element = in_stream.read(blocksize)
        out_stream.write(element)


def rollingchecksum(removed, new, a, b, blocksize=4096):
    a -= removed - new
    b -= removed * blocksize - a
    return (b << 16) | a, a, b


def weak_checksum(data):
    """
    Создаем слабую чексумму.
    """
    a = b = 0
    l = len(data)
    for i in range(l):
        a += data[i]
        b += (l - i)*data[i]


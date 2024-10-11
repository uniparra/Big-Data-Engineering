import scala.annotation.{tailrec, targetName}
import math.BigInt.int2bigInt

// Se observa que para que el problema tenga solucion debe cumplirse que o bien,
//    (i) las capacidades de los vasos, c1 y c2 (c1 < c2), no son coprimos y por tanto el objetivo,l, ha de ser multiplo o divisor del mcd de
//        las capacidades y estar entre ellas, es decir, mcd(c1,c2) = e > 1 & l > e & l%e=0 | e%l=0. O bien,
//    (ii) las capacidades de los vasos, c1 y c2 (c1 < c2), son coprimos y por tanto el objetivo, l, debe ser menor que c2, es decir, l < c2.
// Cabe señalar, que independientemente del numero de vasos dados para resolver el problema si este es resoluble, es decir si estamos en alguno
// de los casos citados, será posible encontrar la solución empleando dos vasos.


/**
 * Clase que representa un vaso con un cierto contenido y capacidad.
 *
 * @param contenido el contenido actual del vaso
 * @param capacidad la capacidad máxima del vaso
 */
case class Vaso(contenido: Int, capacidad: Int){

  // Método para llenar el vaso a su capacidad máxima
  def llenar() = Vaso(this.capacidad, this.capacidad)

  // Método para vaciar el vaso
  def vaciar = Vaso(0, this.capacidad)

  // Método privado para equilibrar el contenido del vaso dentro de los límites válidos
  private def equilibrar() = {
    if (this.contenido < 0) Vaso(0, this.capacidad)
    else if (this.contenido > this.capacidad) Vaso(this.capacidad, this.capacidad)
    else this
  }

  /**
   * Mueve el contenido de un vaso a otro
   *
   * @param vasoDestino el vaso de destino
   * @return una lista de dos vasos después de mover el contenido
   */
  def mover(vasoDestino: Vaso) =
    List(Vaso(this.contenido - (vasoDestino.capacidad - vasoDestino.contenido), this.capacidad).equilibrar(), Vaso(vasoDestino.contenido + this.contenido, vasoDestino.capacidad).equilibrar())

  // Sobrecarga de operadores para comparar vasos por su capacidad
  @targetName("mayor que")
  def <(other: Vaso) = this.capacidad < other.capacidad
  @targetName("menor que")
  private def >(other: Vaso) = this.capacidad > other.capacidad
  @targetName("igual que")
  def ==(other: Vaso) = ! <(other) && ! >(other)

  // Método para representar el vaso como una cadena
  override def toString: String = s"Vaso_de_${this.capacidad}L(${this.contenido})"
  def tieneCero = this.contenido==0
}

/**
 * Muestra el proceso de los vasos en formato legible
 *
 * @param vasos lista de vasos a mostrar
 * @return una cadena representando el proceso de los vasos
 */
def mostrarProceso(vasos: List[Vaso]) = vasos.grouped(2).filter(x=> !x.head.tieneCero && !x.last.tieneCero).map(x => "("+ x.map(y => y.toString).mkString(",")+")").mkString(" -> \n")


/**
 * Encuentra el vaso con la capacidad mínima en una lista de vasos
 *
 * @param min el vaso mínimo actual
 * @param xs la lista de vasos a comparar
 * @return el vaso con la capacidad mínima
 */
@tailrec
def minimo(min: Vaso, xs: List[Vaso]): Vaso = xs match {
  case x::xs => if(x<min) minimo(x,xs) else minimo(min,xs)
  case Nil => min
}

/**
 * Ordena una lista de vasos por capacidad en orden ascendente
 *
 * @param xs la lista de vasos a ordenar
 * @return una lista de vasos ordenada
 */
def ordenar(xs: List[Vaso]): List[Vaso] = xs match {
  case x::ys => minimo(x, xs) :: ordenar(xs.filter(_ != minimo(x, xs)))
  case Nil => xs
}

/**
 * Llena el segundo vaso en la lista de vasos
 *
 * @param vaso lista de dos vasos
 * @return una nueva lista de vasos con el segundo vaso lleno
 */
def llenarVaso(vaso: List[Vaso]) = List(vaso.head, vaso(1).llenar())

/**
 * Mueve el contenido del segundo vaso al primer vaso
 *
 * @param vaso lista de dos vasos
 * @return una nueva lista de vasos después de mover el contenido
 */
def volcar(vaso: List[Vaso]) = vaso(1) mover vaso.head

/**
 * Vacía el primer vaso en la lista de vasos
 *
 * @param vaso lista de dos vasos
 * @return una nueva lista de vasos con el primer vaso vacío
 */
def vaciarVaso(vaso: List[Vaso]) = List(vaso.head.vaciar, vaso(1))

/** Función para determinar el movimiento a realizar basado en el estado de los vasos
 *
 * @param vaso lista de los dos vasos resolutores
 * @return lista de los dos vasos aplicada la transformacion
 */
def movimiento(vaso: List[Vaso]) = if(vaso.last.contenido == 0) llenarVaso(vaso)
else if(vaso.head.contenido == 0) volcar(vaso)
else if(vaso.head.contenido != vaso.head.capacidad && vaso.last.contenido == vaso.last.capacidad) volcar(vaso)
else vaciarVaso(vaso)

/**
 * Resuelve el problema.
 *
 * @param vasos lista de vasos iniciales
 * @param objetivo el volumen objetivo a medir
 * @return la lista de vasos que resuelven el problema
 */
def resolvedor(vasos: List[Vaso], objetivo: Int): List[Vaso] =
  vasos.contains(Vaso(objetivo, vasos.head.capacidad)) || vasos.contains(Vaso(objetivo, vasos.last.capacidad)) match {
    case true => vasos
    case false => vasos ++ resolvedor(ordenar(movimiento(vasos)), objetivo)
  }

/**
 * Encuentra los vasos cuyo máximo común divisor con el vaso mayor no es 1.
 *
 * @param vasos lista de vasos
 * @return lista de vasos primos
 */
def vasosPrimos(vasos: List[Vaso]): List[Vaso] = {
  val vasosOrdenados = ordenar(vasos)
  val vasoMayor = vasosOrdenados.last
  val vasosPrim = vasosOrdenados.init.filter(_.capacidad.gcd(vasoMayor.capacidad) != 1)

  if (vasosPrim.isEmpty) vasosPrimos(vasosOrdenados.init)
  else Set(vasosPrim.head, vasoMayor).toList
}

/**
 * Verifica si un número es múltiplo de otro.
 *
 * @param a el primer número
 * @param b el segundo número
 * @return true si a es múltiplo de b o viceversa, false en caso contrario
 */
def esMultiplo(a: Int, b: Int) = if(a > b) a % b == 0 else b % a == 0

/**
 * Elige los vasos más adecuados para resolver el problema.
 *
 * @param vasos lista de vasos
 * @param objetivo el volumen objetivo a medir
 * @return lista de vasos seleccionados
 */
def eleccionVasos(vasos: List[Vaso], objetivo: Int): List[Vaso] = {
  if(vasos.length > 2) {
    val vasoOrdenada = ordenar(vasos)
    val vasosPrim = vasosPrimos(vasos)
    val mcdVasos = vasosPrim.head.capacidad.gcd(vasosPrim.last.capacidad).toInt
    if (vasosPrim.length > 1 && esMultiplo(objetivo, mcdVasos) && mcdVasos < objetivo) vasosPrim
    else {
      List({
        for {
          x <- ordenar(vasoOrdenada).init
          if x.capacidad.gcd(ordenar(vasoOrdenada).last.capacidad) == 1
        } yield x
      }.head, ordenar(vasoOrdenada).last)
    }
  }
  else vasos
}

// Definición de los vasos iniciales y el objetivo
val vasosIniciales = List(Vaso(0, 4), Vaso(0, 9), Vaso(0, 16))
val objetivo = 6

// Selección de los vasos adecuados y resolución del problema
val vasosEle = eleccionVasos(vasosIniciales, objetivo)
println(mostrarProceso(resolvedor(vasosEle, objetivo)))

def pruebaGit="hola hola"






//def aplicarMov(ciclo: List[List[Vaso]=>List[Vaso]], vasoInicial: List[Vaso]) = ciclo.foldLeft(vasoInicial)((v, movimiento) => ordenar(movimiento(v)))

//(1 until 16).map(x=>aplicarMov(LazyList.continually(List(llenarVaso,volcar,vaciarVaso,volcar,vaciarVaso,volcar)).flatten.take(x).toList,vasosEle)).toList